// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"


	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	//real election interval
	realElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	//vote counter
	voteCount int

	//denial vote counter
	denialCount int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &Raft{
		id: c.ID,
		Lead: None,
		Vote: hardState.Vote,
		Term: hardState.Term,
		RaftLog: newLog(c.Storage),
		Prs: make(map[uint64]*Progress),
		State: StateFollower,
		votes: make(map[uint64]bool),
		voteCount: 0,
		denialCount: 0,
		msgs: make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout: c.ElectionTick,
	}
	lastIndex := r.RaftLog.LastIndex()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	//r.Prs[r.id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
	for _, id := range c.peers {
		if id == r.id {
			r.Prs[r.id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		} else {
			r.Prs[id] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	r.becomeFollower(0, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	preLogIndex := r.Prs[to].Next - 1
	if lastIndex < preLogIndex {
		return true
	}
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		return false
	}
	entries,_:= r.RaftLog.Entries(preLogIndex+1,lastIndex+1)
	sendEntries := make([]*pb.Entry, 0)
	for _, en := range entries {
		sendEntries = append(sendEntries, &pb.Entry{
			EntryType: en.EntryType,
			Term: en.Term,
			Index: en.Index,
			Data: en.Data,
		})
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: r.id,
		Term: r.Term,
		LogTerm: preLogTerm,
		Index: preLogIndex,
		Entries: sendEntries,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgSnapshot,
		From: r.id,
		To: to,
		Term: r.Term,
		Snapshot: &snap,
	})
	r.Prs[to].Next = snap.Metadata.Index + 1
}

// send sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To: to,
		From: r.id,
		Term: r.Term,
		Index: prevLogIndex,
		LogTerm: prevLogTerm,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

//sendVoteResponse sends a vote response to the given peer
func(r *Raft) sendVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		Reject: reject,
	}
	r.msgs = append(r.msgs, msg)
}

//sendAppendResponse sends a AppendEntries response to the given peer.
func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		Reject: reject,
		Index: r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

//sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r*Raft) sendHeartbeatResponse(to uint64, reject bool) {
	logIndex := r.RaftLog.LastIndex()
	logTerm,_ := r.RaftLog.Term(logIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From: r.id,
		To: to,
		Term: r.Term,
		Reject: reject,
		Index: logIndex,
		LogTerm: logTerm,
	}
	r.msgs = append(r.msgs, msg)
} 

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if(r.isLeader()) {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	log.Debugf("%v become follower", r.id)
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.votes = nil
	r.voteCount = 0
	r.leadTransferee = None
	r.resetTick()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Debugf("%v become candidate", r.id)
	
	r.State = StateCandidate
	r.Term += 1
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.voteCount = 1
	//reset heartbeat and election tick
	r.resetTick()
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	log.Debugf("%v become leader", r.id)
	r.State = StateLeader
	r.Lead = r.id
	//reset log replication progress of peers
	for _, p := range r.Prs {
		p.Match = 0
		p.Next = r.RaftLog.LastIndex() + 1
	}
	r.resetTick()
	// NOTE: Leader should propose a noop entry on its term to announce its leadership
	r.appendEntries()
	r.updateCommit()
	
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//reply false if term < currentTerm
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	//reply false if log doesn't contain an entry at prevLogIndex or the term mismatch
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendAppendResponse(m.From, true)
		return
	}
	// if an existing entry conflicts with a new one(same index but different terms)
	if len(m.Entries) > 0 {
		appendStart := 0
		for i, ent := range m.Entries {
			if ent.Index > r.RaftLog.LastIndex(){
				appendStart = i
				break
			}
			isValidTerm, _ := r.RaftLog.Term(ent.Index)
			if isValidTerm != ent.Term {
				r.RaftLog.removeEntriesAfter(ent.Index) //remove entry from ent.Index to last
				break
			}
			appendStart = i
		}
		//log replication: append after
		if m.Entries[appendStart].Index > r.RaftLog.LastIndex() {
			for _, e := range m.Entries[appendStart:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *e)
			}
		}
	}
	//if leaderCommit > commitIndex
	//set commitIndex = min(leaderCommit, index of last new entry)
	if m.Commit > r.RaftLog.committed {
		lastNewEntryIndex := m.Index
		if len(m.Entries) > 0 {
			lastNewEntryIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntryIndex)
	}
	r.sendAppendResponse(m.From, false)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//reply false if term < current term
	if m.Term < r.Term{
		r.sendHeartbeatResponse(m.From, false)
		return
	}
	r.becomeFollower(m.Term, m.From)
	//reply false if log doesn't contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	//update committed
	if m.Commit > r.RaftLog.committed{
		r.RaftLog.committed = min(m.Commit, r.RaftLog.committed)
	}
	r.sendHeartbeatResponse(m.From, false)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

//handleLeaderTransfer handle Leader transfer RPC request
func (r *Raft) handleLeaderTransfer(m pb.Message) {
	
}

// handleVoteRequest handle VoteRequest RPC request
func (r *Raft) handleVoteRequest(m pb.Message) {
	//reject voting
	if r.Term > m.Term {
		r.sendVoteResponse(m.From, true)
		return
	}
	//candidate's log is more up to date than votee, reject voting
	if r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		//candidate can't be leader in this term, become Follower and novoting
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResponse(m.From, true)
		return
	}
	//definitely loser in this term, vote to votee and become follower
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.Vote == m.From {
		r.sendVoteResponse(m.From, false)
		return
	}
	//loser also has right for voting
	if r.isFollower() && r.Vote == None &&
		(!r.isMoreUpToDateThan(m.LogTerm, m.Index)) {
			//vote
			r.sendVoteResponse(m.From, false)
			return
	}
	r.resetRealElectionTimeout() 
	r.sendVoteResponse(m.From, true)
}

//handleVoteResponse handle VoteReponse RPC response
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, r.Lead)
		r.Vote = m.From
		return
	}
	if !m.Reject{
		r.votes[m.From] = true
		r.voteCount += 1
	} else {
		r.votes[m.From] = false
		r.denialCount += 1
	}
	if r.voteCount > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.denialCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

//handleAppendResponse handle AppendReponse RPC response
func(r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if !m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	}else if r.Prs[m.From].Next > 0 {
		r.Prs[m.From].Next -= 1
		//send AppenEntries RPC repeatly until find the matched entry log
		r.sendAppend(m.From)
		return
	}
	r.updateCommit()

	

}

//handleHeartBeatReponse handle hartbeatReponse RPC response
func(r *Raft) handleHeartBeatReponse(m pb.Message){
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	//leader should send log to follower when it received a heartbeat reponse
	//which indicate that it doesn't have update-to-date log
	if m.Reject && r.isMoreUpToDateThan(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType{
	case pb.MessageType_MsgPropose:
		lastIndex := r.RaftLog.LastIndex()
		ents := make([]*pb.Entry,0)
		for _, e := range m.Entries {
			ents = append(ents, &pb.Entry{
				EntryType: e.EntryType,
				Term: r.Term,
				Index: lastIndex + 1,
				Data: e.Data,
			})
			lastIndex += 1
		}
		r.appendEntries(ents...)
		//broadcast appendrequest RPC and update commit 
		r.bcastAppend()
		r.updateCommit()
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatReponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleLeaderTransfer(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow: //let the leader transfer target timeout and start a new election
		r.startElection()
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}


func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType{
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
	return nil
}





//---------------------Auxiliary Function---------------
 func (r *Raft) isLeader() bool{
	return r.State == StateLeader
 }

 func (r *Raft) isFollower() bool {
	return r.State == StateFollower
 }

 func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed += 1
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		//broadcast heartbeat msgs
		r.bcastHeartbeat()
	}
 }

 func (r *Raft) tickElection() {
	r.electionElapsed += 1
	if r.electionElapsed == r.realElectionTimeout {
		r.electionElapsed = 0
		//start leader election
		r.startElection()
	}
 }

 func (r *Raft) resetRealElectionTimeout() {
	//放松选举超时范围
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
 }

 func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRealElectionTimeout()
 }

 func (r *Raft) bcastHeartbeat() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
 }
 func (r *Raft) bcastVoteRequest() {
	lastIndex := r.RaftLog.	LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	for peer := range r.Prs {
		if peer != r.id {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From: r.id,
				To: peer,
				Term: r.Term,
				LogTerm: lastTerm,
				Index: lastIndex,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
 }

 func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
 }
//leader append proposed Entries
 func (r *Raft) appendEntries(entries ...*pb.Entry){
	ents := make([]pb.Entry, 0)
	for _, e := range entries {
		if e.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = e.Index
		}
		ents = append(ents, pb.Entry{
			EntryType: e.EntryType,
			Term: e.Term,
			Index: e.Index,
			Data: e.Data,
		})
	}
	r.RaftLog.appendEntries(ents...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
 }

 //become candidate and broatcast requestVote msgs
 func (r *Raft) startElection() {
	r.becomeCandidate()
	r.bcastVoteRequest()
 }

 func (r *Raft) updateCommit() {
	commitUpdated := false
	for i := r.RaftLog.committed; i < r.RaftLog.LastIndex(); i++ {
		if i <= r.RaftLog.committed {
			continue
		}
		matchCnt := 0;
		for _, p := range r.Prs{
			if i <= p.Match {
				matchCnt += 1
			}
		}
		term, _ := r.RaftLog.Term(i)
		// leader only commit on its current term to avoid overwritten commited log 
		if matchCnt > len(r.Prs)/2 && term == r.Term && r.RaftLog.committed != i {
			r.RaftLog.committed = i
			commitUpdated = true
		}
	}
	//ask followers to update raftlog.Commit field
	if commitUpdated {
		r.bcastAppend()
	}
 }
 func (r *Raft) isMoreUpToDateThan(logTerm, index uint64) bool {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if lastTerm > logTerm || (lastTerm == logTerm && r.RaftLog.LastIndex() > index) {
		return true
	}
	return false
 }
 
 