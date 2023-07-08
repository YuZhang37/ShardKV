package shardController

import (
	"fmt"
	"sort"
)

/****************** for Join *******************/

func (sc *ShardController) removeDupGIDAndCheckDupServersForJoin(command *ControllerCommand, reply *ControllerReply) bool {
	joinedGroups := make(map[int]bool)
	joinedServernames := make(map[string]int)
	for gid, servernames := range command.JoinedGroups {
		if _, exists := joinedGroups[gid]; exists {
			continue
		}
		joinedGroups[gid] = true
		for _, servername := range servernames {
			if existInGid, exists := joinedServernames[servername]; exists {
				reply.ErrorCode = JOIN_SERVERDUPINREQUEST
				reply.ErrorMessage = fmt.Sprintf(JOIN_SERVERDUPINREQUEST_MESSAGE, servername, existInGid)
				return false
			}
		}
	}
	return true
}

func (sc *ShardController) addToConfig(config *Config, joinedGroups map[int][]string) {

	oldGIDs := sc.sortedGIDs(config.Groups)
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

	groupShards := config.GroupShards
	freedShards := make([]int, 0)
	// to accommodate init case
	for shard, gid := range config.Shards {
		if gid == 0 {
			// no group manages this shard
			freedShards = append(freedShards, shard)
		}
	}
	i := 0
	for i = 0; i < rShards && i < len(oldGIDs); i++ {
		gid := oldGIDs[i]
		if len(groupShards[gid]) > tShards+1 {
			freedShards = append(freedShards, groupShards[gid][(tShards+1):]...)
			groupShards[gid] = groupShards[gid][:(tShards + 1)]
		}
	}
	for ; i < len(oldGIDs); i++ {
		gid := oldGIDs[i]
		if len(groupShards[gid]) > tShards {
			freedShards = append(freedShards, groupShards[gid][tShards:]...)
			groupShards[gid] = groupShards[gid][:tShards]
		}
	}
	sort.Ints(freedShards)
	sc.tempDPrintf("Before proccessing Join, Got freedShards: %v", freedShards)
	j := 0
	for j = 0; i < rShards; i, j = i+1, j+1 {
		groupShards[joinedGIDs[j]] = freedShards[:(tShards + 1)]
		for k := 0; k < tShards+1; k++ {
			config.Shards[freedShards[k]] = joinedGIDs[j]
		}
		freedShards = freedShards[(tShards + 1):]
	}
	for ; j < len(joinedGIDs); j++ {
		groupShards[joinedGIDs[j]] = freedShards[:tShards]
		for k := 0; k < tShards; k++ {
			config.Shards[freedShards[k]] = joinedGIDs[j]
		}
		freedShards = freedShards[tShards:]
	}
	sc.tempDPrintf("After proccessing Join, Got freedShards: %v", freedShards)

}

func (sc *ShardController) sortedGIDs(groups map[int][]string) []int {
	gids := make([]int, 0)
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}

func (sc *ShardController) checkNoExistsForJoin(nextConfig *Config, command *ControllerCommand, reply *ControllerReply) bool {
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

func (sc *ShardController) removeFromConfig(config *Config, leaveGIDs []int) {
	freedShards := make([]int, 0)
	groupShards := config.GroupShards
	for _, gid := range leaveGIDs {
		servernames := config.Groups[gid]
		delete(config.Groups, gid)
		for _, servername := range servernames {
			delete(config.ServerNames, servername)
		}
		shards := config.GroupShards[gid]
		delete(config.GroupShards, gid)
		freedShards = append(freedShards, shards...)
	}
	sc.tempDPrintf("Before proccessing Leave, Got freedShards: %v", freedShards)

	tShards := NShards / len(config.Groups)
	rShards := NShards % len(config.Groups)

	// r groups with t + 1, n - r groups with t
	remainGIDs := sc.sortedGIDs(config.Groups)
	sort.Ints(freedShards)

	i := 0
	for i = 0; i < rShards; i++ {
		gid := remainGIDs[i]
		diff := tShards + 1 - len(groupShards[gid])
		if diff == 0 {
			continue
		}
		groupShards[gid] = append(groupShards[gid], freedShards[:diff]...)

		// not necessary
		sort.Ints(groupShards[gid])

		for j := 0; j < diff; j++ {
			config.Shards[freedShards[j]] = gid
		}
		freedShards = freedShards[diff:]
	}
	for ; i < len(remainGIDs); i++ {
		gid := remainGIDs[i]
		diff := tShards - len(groupShards[gid])
		if diff == 0 {
			continue
		}
		groupShards[gid] = append(groupShards[gid], freedShards[:diff]...)
		sort.Ints(groupShards[gid])
		for j := 0; j < diff; j++ {
			config.Shards[freedShards[j]] = gid
		}
		freedShards = freedShards[diff:]
	}
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
