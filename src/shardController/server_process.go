package shardController

func (sc *ShardController) processJoin(command ControllerCommand) *ControllerReply {
	sc.processPrintf(true, "Join", command)
	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
	}

	latestConfig := sc.configs[len(sc.configs)-1]
	nextConfig := sc.copyConfig(&latestConfig)
	nextConfig.Num++

	// client error shoult move to client end
	if !sc.removeDupGIDAndCheckDupServersForJoin(&command, &reply) {
		return &reply
	}
	if !sc.checkNoExistsForJoin(nextConfig, &command, &reply) {
		return &reply
	}
	sc.addToConfig(nextConfig, command.JoinedGroups)
	sc.configs = append(sc.configs, *nextConfig)
	sc.processPrintf(false, "Join", reply)
	return &reply
}

func (sc *ShardController) processLeave(command ControllerCommand) *ControllerReply {
	sc.processPrintf(true, "Leave", command)
	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
	}
	latestConfig := sc.configs[len(sc.configs)-1]
	nextConfig := sc.copyConfig(&latestConfig)
	nextConfig.Num++
	sc.removeDupGIDsForLeave(&command)
	if !sc.checkExistsForLeave(nextConfig, &command, &reply) {
		return &reply
	}
	if len(command.LeaveGIDs) == len(nextConfig.Groups) {
		sc.initConfig(nextConfig.Num)
		return &reply
	}
	sc.removeFromConfig(nextConfig, command.LeaveGIDs)
	sc.configs = append(sc.configs, *nextConfig)
	sc.processPrintf(false, "Leave", reply)
	return &reply
}

func (sc *ShardController) processMove(command ControllerCommand) *ControllerReply {
	sc.processPrintf(true, "Move", command)
	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
	}
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
	nextConfig.Shards[command.MovedShard] = command.MovedGID
	sc.configs = append(sc.configs, *nextConfig)
	sc.processPrintf(false, "Move", reply)
	return &reply
}

func (sc *ShardController) copyConfig(from *Config) *Config {
	to := Config{}
	to.Num = from.Num
	to.Shards = from.Shards
	to.Groups = make(map[int][]string)
	to.GroupShards = make(map[int][]int)
	to.ServerNames = make(map[string]int)
	for key, value := range from.Groups {
		to.Groups[key] = make([]string, 0)
		to.Groups[key] = append(to.Groups[key], value...)
	}
	for key, value := range from.GroupShards {
		to.GroupShards[key] = make([]int, 0)
		to.GroupShards[key] = append(to.GroupShards[key], value...)
	}
	for key, value := range from.ServerNames {
		to.ServerNames[key] = value
	}
	return &to
}

func (sc *ShardController) processQuery(command ControllerCommand) *ControllerReply {
	sc.processPrintf(true, "Query", command)
	reply := ControllerReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: sc.me,

		Succeeded: true,
		ErrorCode: NOERROR,
	}
	if command.QueryNum == -1 || command.QueryNum >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	} else {
		reply.Config = sc.configs[command.QueryNum]
	}
	sc.processPrintf(false, "Query", reply)
	return &reply
}
