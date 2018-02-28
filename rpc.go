package raft

// 日志追加
type appendEntriesTuple struct {
	// 日志追加请求
	Request  appendEntries
	Response chan appendEntriesResponse
}

// 投票选举
type requestVoteTuple struct {
	// 选举内容
	Request  requestVote
	Response chan requestVoteResponse
}

// 日志追加实体
// appendEntries represents an appendEntries RPC.
type appendEntries struct {
	// 任期号
	Term         uint64     `json:"term"`
	// Leader标识
	LeaderID     uint64     `json:"leader_id"`
	// 前一个日志索引
	PrevLogIndex uint64     `json:"prev_log_index"`
	// 前一个日志任期号
	PrevLogTerm  uint64     `json:"prev_log_term"`	
	// 实体数组
	Entries      []logEntry `json:"entries"`
	// 已经committed的索引
	CommitIndex  uint64     `json:"commit_index"`
}

// 日志追加应答
// appendEntriesResponse represents the response to an appendEntries RPC.
type appendEntriesResponse struct {
	// 应答节点任期号
	Term    uint64 `json:"term"`
	// 是否追加成功
	Success bool   `json:"success"`
	// 追加失败的原因
	reason  string
}

// 投票请求实体
// requestVote represents a requestVote RPC.
type requestVote struct {
	// 候选人的任期号
	Term         uint64 `json:"term"`
	// 候选人ID
	CandidateID  uint64 `json:"candidate_id"`
	// 候选人最后一条日志的条目
	LastLogIndex uint64 `json:"last_log_index"`
	// 候选人最后一条日志的任期
	LastLogTerm  uint64 `json:"last_log_term"`
}

// 投票应答
// requestVoteResponse represents the response to a requestVote RPC.
type requestVoteResponse struct {
	// 应答者任期号
	Term        uint64 `json:"term"`
	// 应答结果：true为赞同，false为反对
	VoteGranted bool   `json:"vote_granted"`
	// 反对原因
	reason      string
}
