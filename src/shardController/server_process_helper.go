package shardController

import (
	"fmt"
	"sort"
)

/****************** for Join *******************/

// check if the request has dup servernames
// return pass or not
func (sc *ShardController) checkDupServernamesForJoin(command *ControllerCommand, reply *ControllerReply) bool {
	joinedServernames := make(map[string]int)
	for gid, servernames := range command.JoinedGroups {
		for _, servername := range servernames {
			if existInGid, exists := joinedServernames[servername]; exists {
				reply.ErrorCode = JOIN_SERVERDUPINREQUEST
				reply.ErrorMessage = fmt.Sprintf(JOIN_SERVERDUPINREQUEST_MESSAGE, servername, existInGid)
				return false
			} else {
				joinedServernames[servername] = gid
			}
		}
	}
	return true
}

// check if gid or servernames exist in current config:
// return pass or not
func (sc *ShardController) checkExistsForJoin(nextConfig *Config, command *ControllerCommand, reply *ControllerReply) bool {
	for gid, servers := range command.JoinedGroups {
		if _, exists := nextConfig.Groups[gid]; exists {
			reply.ErrorCode = JOIN_GIDEXISTS
			reply.ErrorMessage = fmt.Sprintf(JOIN_GIDEXISTS_MESSAGE, gid)
			return false
		}
		for _, servername := range servers {
			if gid0, exists := nextConfig.ServerNames[servername]; exists {
				reply.ErrorCode = JOIN_SERVEREXISTS
				reply.ErrorMessage = fmt.Sprintf(JOIN_SERVEREXISTS_MESSAGE, servername, gid0)
				return false
			}
		}
	}
	return true
}

func (sc *ShardController) addToConfig(config *Config, joinedGroups map[int][]string) {

	joinedGIDs := sc.sortedGIDs(joinedGroups)

	for gid, servernames := range joinedGroups {
		config.Groups[gid] = make([]string, 0)
		for _, servername := range servernames {
			config.ServerNames[servername] = gid
			config.Groups[gid] = append(config.Groups[gid], servername)
		}
	}
	tShards := NShards / len(config.Groups)
	rShards := NShards % len(config.Groups)
	// r groups with t + 1, n - r groups with t

	freedShards := sc.collectFreedShardsForJoin(config, tShards, rShards)
	sc.tempDPrintf("Before proccessing Join %v, Got freedShards: %v, tShards: %v, rShards: %v\n", freedShards, joinedGIDs, tShards, rShards)

	i := len(config.GroupInfos)
	j := 0
	for j = 0; i < rShards; i, j = i+1, j+1 {
		groupInfo := GroupInfo{
			GID:        joinedGIDs[j],
			JoinedTerm: config.Num,
			Shards:     make([]int, 0),
		}
		groupInfo.Shards = freedShards[:(tShards + 1)]
		config.GroupInfos = append(config.GroupInfos, groupInfo)
		for k := 0; k < tShards+1; k++ {
			config.Shards[freedShards[k]] = joinedGIDs[j]
		}
		freedShards = freedShards[(tShards + 1):]
	}

	for ; j < len(joinedGIDs); j++ {
		groupInfo := GroupInfo{
			GID:        joinedGIDs[j],
			JoinedTerm: config.Num,
			Shards:     make([]int, 0),
		}
		groupInfo.Shards = freedShards[:tShards]
		config.GroupInfos = append(config.GroupInfos, groupInfo)
		for k := 0; k < tShards; k++ {
			config.Shards[freedShards[k]] = joinedGIDs[j]
		}
		freedShards = freedShards[tShards:]
	}
	sc.tempDPrintf("After proccessing Join, Got freedShards: %v", freedShards)

}

/*
return a list of freed shards
*/
func (sc *ShardController) collectFreedShardsForJoin(config *Config, tShards int, rShards int) []int {
	freedShards := make([]int, 0)
	// collect shards from init case
	for shard, gid := range config.Shards {
		if gid == 0 {
			// no group manages this shard
			freedShards = append(freedShards, shard)
		}
	}
	i := 0
	for i = 0; i < rShards && i < len(config.GroupInfos); i++ {
		group := config.GroupInfos[i]
		if len(group.Shards) > tShards+1 {
			freedShards = append(freedShards, group.Shards[(tShards+1):]...)
			config.GroupInfos[i].Shards = group.Shards[:(tShards + 1)]
		}
	}
	// if reached a separation line of (t+1) | (t)
	for ; i < len(config.GroupInfos); i++ {
		group := config.GroupInfos[i]
		if len(group.Shards) > tShards {
			freedShards = append(freedShards, group.Shards[(tShards):]...)
			config.GroupInfos[i].Shards = group.Shards[:tShards]
		}
	}
	// i will always be len(config.GroupInfos) at this point
	sort.Ints(freedShards)
	return freedShards
}

func (sc *ShardController) sortedGIDs(groups map[int][]string) []int {
	gids := make([]int, 0)
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}

/******************* for leave ********************/

func (sc *ShardController) removeDupGIDsForLeave(command *ControllerCommand) {
	dedupedGIDs := make([]int, 0)
	leaveGroups := make(map[int]bool)
	for _, gid := range command.LeaveGIDs {
		if _, exists := leaveGroups[gid]; !exists {
			leaveGroups[gid] = true
			dedupedGIDs = append(dedupedGIDs, gid)
		}
	}
	command.LeaveGIDs = dedupedGIDs
}

func (sc *ShardController) checkExistsForLeave(nextConfig *Config, command *ControllerCommand, reply *ControllerReply) bool {
	for _, gid := range command.LeaveGIDs {
		if _, exists := nextConfig.Groups[gid]; !exists {
			reply.ErrorCode = LEAVE_GIDNOTEXISTS
			reply.ErrorMessage = fmt.Sprintf(LEAVE_GIDNOTEXISTS_MESSAGE, gid)
			return false
		}
	}
	return true
}

func (sc *ShardController) removeFromConfig(config *Config, leaveGIDs []int) {
	freedShards := sc.collectFreedShardsAndLeaveGIDs(config, leaveGIDs)

	tShards := NShards / len(config.Groups)
	rShards := NShards % len(config.Groups)
	sc.tempDPrintf("Before proccessing Leave: %v, Got freedShards: %v, tShards: %v, rShards: %v\n current config: %v\n", leaveGIDs, freedShards, tShards, rShards, config)
	i := 0
	for i = 0; i < rShards; i++ {
		diff := tShards + 1 - len(config.GroupInfos[i].Shards)
		if diff == 0 {
			continue
		}
		config.GroupInfos[i].Shards = append(config.GroupInfos[i].Shards, freedShards[:diff]...)
		sort.Ints(config.GroupInfos[i].Shards)
		for j := 0; j < diff; j++ {
			config.Shards[freedShards[j]] = config.GroupInfos[i].GID
		}
		freedShards = freedShards[diff:]
	}
	for ; i < len(config.GroupInfos); i++ {
		diff := tShards - len(config.GroupInfos[i].Shards)
		if diff == 0 {
			continue
		}
		config.GroupInfos[i].Shards = append(config.GroupInfos[i].Shards, freedShards[:diff]...)
		sort.Ints(config.GroupInfos[i].Shards)
		for j := 0; j < diff; j++ {
			config.Shards[freedShards[j]] = config.GroupInfos[i].GID
		}
		freedShards = freedShards[diff:]
	}
	sc.tempDPrintf("After proccessing Leave %v, Got freedShards: %v", leaveGIDs, freedShards)
}

func (sc *ShardController) collectFreedShardsAndLeaveGIDs(config *Config, leaveGIDs []int) []int {
	freedShards := make([]int, 0)

	leaveGIDMap := make(map[int]bool, 0)
	for _, gid := range leaveGIDs {
		leaveGIDMap[gid] = true
	}
	newGroupInfos := make([]GroupInfo, 0)
	for _, group := range config.GroupInfos {
		if _, exists := leaveGIDMap[group.GID]; !exists {
			// not a leave gid
			newGroupInfos = append(newGroupInfos, group)
			continue
		}
		// delete servernames
		servernames := config.Groups[group.GID]
		for _, servername := range servernames {
			delete(config.ServerNames, servername)
		}
		// delete group
		delete(config.Groups, group.GID)
		// collect shards
		freedShards = append(freedShards, group.Shards...)
	}
	// update GroupInfos
	config.GroupInfos = newGroupInfos
	sort.Ints(freedShards)
	return freedShards
}

/****************** for Move *******************/
/*
movedShard and movedGID exist
works for initial case with group 0
after move, the balanced pattern is broken,
join and leave will rebalance the pattern
*/
func (sc *ShardController) moveShard(config *Config, movedShard int, movedGID int) {
	config.Moved = true
	originalGroupID := config.Shards[movedShard]
	config.Shards[movedShard] = movedGID
	for _, groupInfo := range config.GroupInfos {
		if groupInfo.GID == movedGID {
			groupInfo.Shards = append(groupInfo.Shards, movedShard)
		}
		if groupInfo.GID == originalGroupID {
			newShards := make([]int, 0)
			for _, shard := range groupInfo.Shards {
				if shard != movedShard {
					newShards = append(newShards, shard)
				}
			}
			groupInfo.Shards = newShards
		}
	}
}

// to remove the effect of move
func (sc *ShardController) balancePattern(config *Config) {
	if !config.Moved {
		return
	}
	config.Moved = false
	tShards := NShards / len(config.Groups)
	rShards := NShards % len(config.Groups)
	freedShards := make([]int, 0)
	i := 0
	for i = 0; i < rShards; i++ {
		if len(config.GroupInfos[i].Shards) > tShards+1 {
			freedShards = append(freedShards, config.GroupInfos[i].Shards[tShards+1:]...)
			config.GroupInfos[i].Shards = config.GroupInfos[i].Shards[:tShards+1]
		}
	}
	for ; i < len(config.Groups); i++ {
		if len(config.GroupInfos[i].Shards) > tShards {
			freedShards = append(freedShards, config.GroupInfos[i].Shards[tShards:]...)
			config.GroupInfos[i].Shards = config.GroupInfos[i].Shards[:tShards]
		}
	}
	sort.Ints(freedShards)
	for i = 0; i < rShards; i++ {
		if len(config.GroupInfos[i].Shards) < tShards+1 {
			diff := tShards + 1 - len(config.GroupInfos[i].Shards)
			config.GroupInfos[i].Shards = append(config.GroupInfos[i].Shards, freedShards[:diff]...)
			freedShards = freedShards[diff:]
		}
	}

	for ; i < len(config.Groups); i++ {
		if len(config.GroupInfos[i].Shards) < tShards {
			diff := tShards - len(config.GroupInfos[i].Shards)
			config.GroupInfos[i].Shards = append(config.GroupInfos[i].Shards, freedShards[:diff]...)
			freedShards = freedShards[diff:]
		}
	}
}
