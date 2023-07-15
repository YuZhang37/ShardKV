package shardController

import "fmt"

/*
the pattern:
all gids are sorted first with joined term in ascending order,
ties are broken up with gids in ascending order
each group has either (t+1) elements or t elements, which is also sorted in ascending order within each group, there is a separation line to separate them to be (t+1) part on the left and (t) part on the right
(t+1)...(t)...
*/

func (sc *ShardController) processJoin(command ControllerCommand) *ControllerReply {
	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
	}
	sc.processPrintf(true, "Join", command, reply)
	latestConfig := sc.configs[len(sc.configs)-1]
	nextConfig := sc.copyConfig(&latestConfig)
	nextConfig.Num++
	nextConfig.Operation = fmt.Sprintf("operation: %v", command)

	// client error should move to client end
	if !sc.checkDupServernamesForJoin(&command, &reply) {
		return &reply
	}
	if !sc.checkExistsForJoin(nextConfig, &command, &reply) {
		return &reply
	}
	sc.balancePattern(nextConfig)
	sc.addToConfig(nextConfig, command.JoinedGroups)
	sc.configs = append(sc.configs, *nextConfig)
	sc.processPrintf(false, "Join", command, reply)
	return &reply
}

func (sc *ShardController) processLeave(command ControllerCommand) *ControllerReply {
	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
	}
	sc.processPrintf(true, "Leave", command, reply)
	latestConfig := sc.configs[len(sc.configs)-1]
	nextConfig := sc.copyConfig(&latestConfig)
	nextConfig.Num++
	nextConfig.Operation = fmt.Sprintf("operation: %v", command)
	// client error should move to client end
	sc.removeDupGIDsForLeave(&command)
	if !sc.checkExistsForLeave(nextConfig, &command, &reply) {
		return &reply
	}
	if len(command.LeaveGIDs) == len(nextConfig.Groups) {
		sc.initConfig(nextConfig.Num)
		return &reply
	}
	sc.balancePattern(nextConfig)
	sc.removeFromConfig(nextConfig, command.LeaveGIDs)
	sc.configs = append(sc.configs, *nextConfig)
	sc.processPrintf(false, "Leave", command, reply)
	return &reply
}

func (sc *ShardController) processMove(command ControllerCommand) *ControllerReply {
	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
	}
	sc.processPrintf(true, "Move", command, reply)
	if command.MovedShard < 0 || command.MovedShard >= NShards {
		reply.ErrorCode = MOVE_NOMOVEDSHARED
		return &reply
	}

	latestConfig := sc.configs[len(sc.configs)-1]
	if _, exists := latestConfig.Groups[command.MovedGID]; !exists {
		reply.ErrorCode = MOVE_NOMOVEDGROUPID
		return &reply
	}
	originalGroupID := latestConfig.Shards[command.MovedShard]
	if originalGroupID == command.MovedGID {
		reply.ErrorCode = MOVE_NOMOVENEEDED
		return &reply
	}

	nextConfig := sc.copyConfig(&latestConfig)
	nextConfig.Num++
	sc.moveShard(nextConfig, command.MovedShard, command.MovedGID)
	sc.configs = append(sc.configs, *nextConfig)
	sc.processPrintf(false, "Move", command, reply)
	nextConfig.Operation = fmt.Sprintf("operation: %v", command)
	return &reply
}

func (sc *ShardController) copyConfig(from *innerConfig) *innerConfig {
	to := innerConfig{}
	to.Num = from.Num
	to.Shards = from.Shards
	to.Groups = make(map[int][]string)
	to.GroupInfos = make([]GroupInfo, 0)
	to.ServerNames = make(map[string]int)
	for key, value := range from.Groups {
		to.Groups[key] = make([]string, 0)
		to.Groups[key] = append(to.Groups[key], value...)
	}
	for _, groupInfo := range from.GroupInfos {
		to.GroupInfos = append(to.GroupInfos, sc.copyGroupInfo(&groupInfo))
	}
	for key, value := range from.ServerNames {
		to.ServerNames[key] = value
	}
	return &to
}

func (sc *ShardController) copyGroupInfo(from *GroupInfo) GroupInfo {
	to := GroupInfo{
		GID:        from.GID,
		JoinedTerm: from.JoinedTerm,
		Shards:     make([]int, 0),
	}
	to.Shards = append(to.Shards, from.Shards...)
	return to
}

func (sc *ShardController) processQuery(command ControllerCommand) *ControllerReply {

	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
		ErrorCode: NOERROR,
	}
	sc.processPrintf(true, "Query", command, reply)
	if command.QueryNum == -1 || command.QueryNum >= len(sc.configs) {
		reply.Config = Config{
			Num:    sc.configs[len(sc.configs)-1].Num,
			Shards: sc.configs[len(sc.configs)-1].Shards,
			Groups: sc.configs[len(sc.configs)-1].Groups,
		}
	} else {
		reply.Config = Config{
			Num:    sc.configs[command.QueryNum].Num,
			Shards: sc.configs[command.QueryNum].Shards,
			Groups: sc.configs[command.QueryNum].Groups,
		}

	}
	sc.processPrintf(false, "Query", command, reply)
	return &reply
}
