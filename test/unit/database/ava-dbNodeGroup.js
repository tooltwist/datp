import test from 'ava'
import { deleteNodeGroup, getNodeGroup, getNodeGroups, saveNodeGroup } from '../../../database/dbNodeGroup'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => { })


test.serial('Get node groups', async t => {
  const groups = await getNodeGroups()
  t.true(groups.length > 0)
  const first = groups[0]
  t.is(first.nodeGroup, 'master')
  t.is(typeof(first.description), 'string')
  t.is(typeof(first.hostMondat), 'number')
  t.is(typeof(first.serveMondatWebapp), 'number')
  t.is(typeof(first.numWorkers), 'number')
  t.is(typeof(first.eventloopPauseBusy), 'number')
  t.is(typeof(first.eventloopPauseIdle), 'number')
  t.is(typeof(first.eventloopPause), 'number')
  t.is(typeof(first.delayToEnterSlothMode), 'number')
  t.is(typeof(first.debugScheduler), 'number')
  t.is(typeof(first.debugWorkers), 'number')
  t.is(typeof(first.debugSteps), 'number')
  t.is(typeof(first.debugPipelines), 'number')
  t.is(typeof(first.debugRouters), 'number')
  t.is(typeof(first.debugLongpolling), 'number')
  t.is(typeof(first.debugWebhooks), 'number')
  t.is(typeof(first.debugTransactions), 'number')
  t.is(typeof(first.debugTransactionCache), 'number')
  t.is(typeof(first.debugRedis), 'number')
  t.is(typeof(first.debugDb), 'number')
})


test.serial('Get master node group', async t => {
  const group = await getNodeGroup('master')
  t.is(group.nodeGroup, 'master')
  t.is(typeof(group.description), 'string')
  t.is(typeof(group.hostMondat), 'number')
  t.is(typeof(group.serveMondatWebapp), 'number')
  t.is(typeof(group.numWorkers), 'number')
  t.is(typeof(group.eventloopPauseBusy), 'number')
  t.is(typeof(group.eventloopPauseIdle), 'number')
  t.is(typeof(group.eventloopPause), 'number')
  t.is(typeof(group.delayToEnterSlothMode), 'number')
  t.is(typeof(group.debugScheduler), 'number')
  t.is(typeof(group.debugWorkers), 'number')
  t.is(typeof(group.debugSteps), 'number')
  t.is(typeof(group.debugPipelines), 'number')
  t.is(typeof(group.debugRouters), 'number')
  t.is(typeof(group.debugLongpolling), 'number')
  t.is(typeof(group.debugWebhooks), 'number')
  t.is(typeof(group.debugTransactions), 'number')
  t.is(typeof(group.debugTransactionCache), 'number')
  t.is(typeof(group.debugRedis), 'number')
  t.is(typeof(group.debugDb), 'number')
})


test.serial('Insert/update/delete node group', async t => {
  const nodeGroup = 'unit-test'

  const DESCRIPTION_1 = 'Test node group. Please delete.'
  const DESCRIPTION_2 = 'Test node group. Please delete. #2'
  const DEFAULT_1 = 1
  const DEFAULT_2 = 0

  let newGroup = {
    nodeGroup,
    description: DESCRIPTION_1,
    hostMondat: DEFAULT_1,
    serveMondatWebapp: DEFAULT_1,
    numWorkers: DEFAULT_1,
    eventloopPauseBusy: DEFAULT_1,
    eventloopPauseIdle: DEFAULT_1,
    eventloopPause: DEFAULT_1,
    delayToEnterSlothMode: DEFAULT_1,
    debugScheduler: DEFAULT_1,
    debugWorkers: DEFAULT_1,
    debugSteps: DEFAULT_1,
    debugPipelines: DEFAULT_1,
    debugRouters: DEFAULT_1,
    debugLongpolling: DEFAULT_1,
    debugWebhooks: DEFAULT_1,
    debugTransactions: DEFAULT_1,
    debugTransactionCache: DEFAULT_1,
    debugRedis: DEFAULT_1,
    debugDb: DEFAULT_1,
  }
  await saveNodeGroup(newGroup)


  let group = await getNodeGroup(nodeGroup)
  t.is(group.hostMondat, DEFAULT_1)
  t.is(group.serveMondatWebapp, DEFAULT_1)
  t.is(group.numWorkers, DEFAULT_1)
  t.is(group.eventloopPauseBusy, DEFAULT_1)
  t.is(group.eventloopPauseIdle, DEFAULT_1)
  t.is(group.eventloopPause, DEFAULT_1)
  t.is(group.delayToEnterSlothMode, DEFAULT_1)
  t.is(group.debugScheduler, DEFAULT_1)
  t.is(group.debugWorkers, DEFAULT_1)
  t.is(group.debugSteps, DEFAULT_1)
  t.is(group.debugPipelines, DEFAULT_1)
  t.is(group.debugRouters, DEFAULT_1)
  t.is(group.debugLongpolling, DEFAULT_1)
  t.is(group.debugWebhooks, DEFAULT_1)
  t.is(group.debugTransactions, DEFAULT_1)
  t.is(group.debugTransactionCache, DEFAULT_1)
  t.is(group.debugRedis, DEFAULT_1)
  t.is(group.debugDb, DEFAULT_1)

  // Now update the values
  let updatedGroup = {
    nodeGroup,
    description: DESCRIPTION_2,
    hostMondat: DEFAULT_2,
    serveMondatWebapp: DEFAULT_2,
    numWorkers: DEFAULT_2,
    eventloopPauseBusy: DEFAULT_2,
    eventloopPauseIdle: DEFAULT_2,
    eventloopPause: DEFAULT_2,
    delayToEnterSlothMode: DEFAULT_2,
    debugScheduler: DEFAULT_2,
    debugWorkers: DEFAULT_2,
    debugSteps: DEFAULT_2,
    debugPipelines: DEFAULT_2,
    debugRouters: DEFAULT_2,
    debugLongpolling: DEFAULT_2,
    debugWebhooks: DEFAULT_2,
    debugTransactions: DEFAULT_2,
    debugTransactionCache: DEFAULT_2,
    debugRedis: DEFAULT_2,
    debugDb: DEFAULT_2,
  }
  await saveNodeGroup(updatedGroup)

  // Check the update
  group = await getNodeGroup(nodeGroup)
  t.is(group.hostMondat, DEFAULT_2)
  t.is(group.serveMondatWebapp, DEFAULT_2)
  t.is(group.numWorkers, DEFAULT_2)
  t.is(group.eventloopPauseBusy, DEFAULT_2)
  t.is(group.eventloopPauseIdle, DEFAULT_2)
  t.is(group.eventloopPause, DEFAULT_2)
  t.is(group.delayToEnterSlothMode, DEFAULT_2)
  t.is(group.debugScheduler, DEFAULT_2)
  t.is(group.debugWorkers, DEFAULT_2)
  t.is(group.debugSteps, DEFAULT_2)
  t.is(group.debugPipelines, DEFAULT_2)
  t.is(group.debugRouters, DEFAULT_2)
  t.is(group.debugLongpolling, DEFAULT_2)
  t.is(group.debugWebhooks, DEFAULT_2)
  t.is(group.debugTransactions, DEFAULT_2)
  t.is(group.debugTransactionCache, DEFAULT_2)
  t.is(group.debugRedis, DEFAULT_2)
  t.is(group.debugDb, DEFAULT_2)

  // Check deleting the node group
  const result = await deleteNodeGroup(nodeGroup)
  t.true(result)
  group = await getNodeGroup(nodeGroup)
  t.is(group, null)
})

test.serial('Delete unknown node group', async t => {
  const result = await deleteNodeGroup('lkjsdhflkjha')
  t.false(result)
})
