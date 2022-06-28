import {
  Action,
  ActionFilter,
  ActionStatus,
  AllocationManagementMode,
  AllocationStatus,
  IndexerManagementModels,
  isActionFailure,
  NetworkMonitor,
} from '@graphprotocol/indexer-common'
import { AllocationManager } from './allocations'
import { Transaction } from 'sequelize'
import { Eventual, join, Logger, timer } from '@graphprotocol/common-ts'

export class ActionManager {
  constructor(
    public allocationManager: AllocationManager,
    public networkMonitor: NetworkMonitor,
    private currentEpoch: number,
    private maxAllocationEpoch: number,
    private logger: Logger,
    private models: IndexerManagementModels,
    private allocationManagementMode?: AllocationManagementMode,
    private autoMinBatchSize?: number,
    private autoMaxWaitTime?: number,
  ) {}

  private async batchReady(
    approvedActions: Action[],
    pollingFrequency: number,
  ): Promise<boolean> {
    if (approvedActions.length < 1) {
      return false
    }

    // In AUTO mode, worker waits to execute when 1) the batch is not filled upto minimum size
    // and 2) oldest allocatation is not expiring after the current epoch
    // and 3) oldest action is not stale (10 times the user provided threshold of polling frequency)
    if (this.allocationManagementMode === AllocationManagementMode.AUTO) {
      const oldestCreatedAtEpoch = (
        await this.networkMonitor.allocations(AllocationStatus.ACTIVE)
      )[0].createdAtEpoch

      const updatedDeadline = new Date(new Date().valueOf() - 10 * pollingFrequency)

      this.logger.debug('Worker in AUTO, checking batch conditions', {
        length: approvedActions.length,
        lengthRequirement: this.autoMinBatchSize ? this.autoMinBatchSize : 1,
        oldestCreatedAtEpoch: oldestCreatedAtEpoch,
        epochRequirement: this.currentEpoch - this.maxAllocationEpoch,
        oldestUpdate: approvedActions.slice(-1)[0].updatedAt,
        updateRequirement: updatedDeadline,
      })

      return !(
        approvedActions.length < (this.autoMinBatchSize ? this.autoMinBatchSize : 1) &&
        this.currentEpoch <= oldestCreatedAtEpoch + this.maxAllocationEpoch &&
        updatedDeadline <= approvedActions.slice(-1)[0].updatedAt
      )
    }

    return true
  }

  async monitorQueue(): Promise<void> {
    const pollingFrequency = 1000 * 60 * (this.autoMaxWaitTime ? this.autoMaxWaitTime : 1)

    const approvedActions: Eventual<Action[]> = timer(pollingFrequency).tryMap(
      async () =>
        await this.models.Action.findAll({
          where: { status: ActionStatus.APPROVED },
        }),
      {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        onError: (err: any) =>
          this.logger.warn('Failed to fetch approved actions from queue', { err }),
      },
    )

    join({ approvedActions }).pipe(async ({ approvedActions }) => {
      if (await this.batchReady(approvedActions, pollingFrequency)) {
        this.logger.info('Executing batch of approved actions', {
          actions: approvedActions,
          note: 'If actions were approved very recently they may be missing from this list but will still be taken',
        })

        const attemptedActions = await this.executeApprovedActions()

        this.logger.trace('Attempted to execute all approved actions', {
          actions: attemptedActions,
        })
      } else {
        this.logger.debug(
          'Worker waiting for the current batch to be ready, simply monitor the queue for now',
        )
      }
    })
  }

  async executeApprovedActions(): Promise<Action[]> {
    let updatedActions: Action[] = []

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    await this.models.Action.sequelize!.transaction(
      { isolationLevel: Transaction.ISOLATION_LEVELS.SERIALIZABLE },
      async (transaction) => {
        // Execute already approved actions in the order of type and priority
        const actionTypePriority = ['unallocate', 'reallocate', 'allocate']
        const approvedActions = (
          await this.models.Action.findAll({
            where: { status: ActionStatus.APPROVED },
            order: [
              // ['type', 'DESC'],
              ['priority', 'ASC'],
            ],
            transaction,
            lock: transaction.LOCK.UPDATE,
          })
        ).sort(function (a, b) {
          return actionTypePriority.indexOf(a.type) - actionTypePriority.indexOf(b.type)
        })

        try {
          // This will return all results if successful, if failed it will return the failed actions
          const results = await this.allocationManager.executeBatch(approvedActions)

          this.logger.debug('Completed batch action execution', {
            results,
          })

          for (const result of results) {
            const status = isActionFailure(result)
              ? ActionStatus.FAILED
              : ActionStatus.SUCCESS
            const [, updatedAction] = await this.models.Action.update(
              {
                status: status,
                transaction: result.transactionID,
                failureReason: isActionFailure(result) ? result.failureReason : null,
              },
              {
                where: { id: result.actionID },
                returning: true,
                transaction,
              },
            )
            updatedActions = updatedActions.concat(updatedAction)
          }
        } catch (error) {
          this.logger.error(`Failed to execute batch tx on staking contract: ${error}`)
          return []
        }
      },
    )

    return updatedActions
  }

  async fetchActions(filter: ActionFilter): Promise<Action[]> {
    const filterObject = JSON.parse(JSON.stringify(filter))
    const queryResult = await this.models.Action.findAll({
      where: filterObject,
      order: [['updatedAt', 'DESC']],
    })
    return queryResult
  }
}
