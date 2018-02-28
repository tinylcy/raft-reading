package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"
)

var (
	// 任期号太小
	errTermTooSmall    = errors.New("term too small")
	// 日志索引太小
	errIndexTooSmall   = errors.New("index too small")
	// 日志索引太大
	errIndexTooBig     = errors.New("commit index too big")
	// 日志条目内容已损坏
	errInvalidChecksum = errors.New("invalid checksum")
	// 无效的命令
	errNoCommand       = errors.New("no command")
	// 错误的日志索引
	errBadIndex        = errors.New("bad index")
	// 错误任期号
	errBadTerm         = errors.New("bad term")
)

// 日志结构
type raftLog struct {
	// 日志读写锁
	sync.RWMutex
	// 日志存储接口
	store     io.Writer
	// 日志镜像，存储于内存
	entries   []logEntry
	// 下一条日志 commit 索引
	commitPos int
	// `操作`回调函数
	apply     func(uint64, []byte) []byte
}

func newRaftLog(store io.ReadWriter, apply func(uint64, []byte) []byte) *raftLog {
	l := &raftLog{
		store:     store,
		entries:   []logEntry{},
		commitPos: -1, // no commits to begin with
		apply:     apply,
	}
	l.recover(store)
	return l
}

// 将存储在持久设备的日志条目恢复到内存中。 
// recover reads from the log's store, to populate the log with log entries
// from persistent storage. It should be called once, at log instantiation.
func (l *raftLog) recover(r io.Reader) error {
	for {
		var entry logEntry
		switch err := entry.decode(r); err {
		case io.EOF:
			return nil // successful completion
		case nil:
			if err := l.appendEntry(entry); err != nil {
				return err
			}
			l.commitPos++
			// 将日志应用到状态机
			l.apply(entry.Index, entry.Command)
		default:
			return err // unsuccessful completion
		}
	}
}

// 获取索引 index 之后的所有日志条目。
// entriesAfter returns a slice of log entries after (i.e. not including) the
// passed index, and the term of the log entry specified by index, as a
// convenience to the caller. (This function is only used by a leader attempting
// to flush log entries to its followers.)
//
// This function is called to populate an AppendEntries RPC. That implies they
// are destined for a follower, which implies the application of the commands
// should have the response thrown away, which implies we shouldn't pass a
// commandResponse channel (see: commitTo implementation). In the normal case,
// the raftLogEntries we return here will get serialized as they pass thru their
// transport, and lose their commandResponse channel anyway. But in the case of
// a LocalPeer (or equivalent) this doesn't happen. So, we must make sure to
// proactively strip commandResponse channels.
func (l *raftLog) entriesAfter(index uint64) ([]logEntry, uint64) {
	l.RLock()
	defer l.RUnlock()

	pos := 0
	lastTerm := uint64(0)
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index > index {
			break
		}
		lastTerm = l.entries[pos].Term
	}

	a := l.entries[pos:]
	if len(a) == 0 {
		return []logEntry{}, lastTerm
	}

	return stripResponseChannels(a), lastTerm
}

func stripResponseChannels(a []logEntry) []logEntry {
	stripped := make([]logEntry, len(a))
	for i, entry := range a {
		stripped[i] = logEntry{
			Index:           entry.Index,
			Term:            entry.Term,
			Command:         entry.Command,
			commandResponse: nil, // 将 commandResponse channel 设置为 nil
		}
	}
	return stripped
}

// 判断日志条目中是否包含特定 index 和 term 的日志项。
// contains returns true if a log entry with the given index and term exists in
// the log.
func (l *raftLog) contains(index, term uint64) bool {
	l.RLock()
	defer l.RUnlock()

	// It's not necessarily true that l.entries[i] has index == i.
	for _, entry := range l.entries {
		if entry.Index == index && entry.Term == term {
			return true
		}
		if entry.Index > index || entry.Term > term {
			break
		}
	}
	return false
}

// 将 {index, term} 之后的日志项全部清空。
// ensureLastIs deletes all non-committed log entries after the given index and
// term. It will fail if the given index doesn't exist, has already been
// committed, or doesn't match the given term.
//
// This method satisfies the requirement that a log entry in an AppendEntries
// call precisely follows the accompanying LastraftLogTerm and LastraftLogIndex.
func (l *raftLog) ensureLastIs(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	// Taken loosely from benbjohnson's impl

	if index < l.getCommitIndexWithLock() {
		return errIndexTooSmall
	}

	if index > l.lastIndexWithLock() {
		return errIndexTooBig
	}

	// Leader 决定重建日志条目，此时不能存在已经 committed 的日志条目。
	// It's possible that the passed index is 0. It means the leader has come to
	// decide we need a complete log rebuild. Of course, that's only valid if we
	// haven't committed anything, so this check comes after that one.
	if index == 0 {
		for pos := 0; pos < len(l.entries); pos++ {
			if l.entries[pos].commandResponse != nil {
				close(l.entries[pos].commandResponse)
				l.entries[pos].commandResponse = nil
			}
			if l.entries[pos].committed != nil {
				l.entries[pos].committed <- false
				close(l.entries[pos].committed)
				l.entries[pos].committed = nil
			}
		}
		l.entries = []logEntry{}
		return nil
	}

	// Normal case: find the position of the matching log entry.
	pos := 0
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index < index {
			continue // didn't find it yet
		}
		if l.entries[pos].Index > index {
			return errBadIndex // somehow went past it
		}
		if l.entries[pos].Index != index {
			panic("not <, not >, but somehow !=")
		}
		if l.entries[pos].Term != term {
			return errBadTerm
		}
		break // good
	}

	// 已经找到满足特定 index 和 term 的日志项。

	// Sanity check.
	if pos < l.commitPos {
		panic("index >= commitIndex, but pos < commitPos")
	}

	// `pos` is the position of log entry matching index and term.
	// We want to truncate everything after that.
	truncateFrom := pos + 1
	if truncateFrom >= len(l.entries) {
		return nil // nothing to truncate
	}

	// 删除 {index, term} 之后的所有日志项。
	// If we blow away log entries that haven't yet sent responses to clients,
	// signal the clients to stop waiting, by closing the channel without a
	// response value.
	for pos = truncateFrom; pos < len(l.entries); pos++ {
		if l.entries[pos].commandResponse != nil {
			close(l.entries[pos].commandResponse)
			l.entries[pos].commandResponse = nil
		}
		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- false
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}
	}

	// Truncate the log.
	l.entries = l.entries[:truncateFrom]

	// Done.
	return nil
}

// 获取最新的已经 committed 的日志项对应的 index。
// getCommitIndex returns the commit index of the log. That is, the index of the
// last log entry which can be considered committed.
func (l *raftLog) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.getCommitIndexWithLock()
}

func (l *raftLog) getCommitIndexWithLock() uint64 {
	if l.commitPos < 0 {
		return 0
	}
	if l.commitPos >= len(l.entries) {
		panic(fmt.Sprintf("commitPos %d > len(l.entries) %d; bad bookkeeping in raftLog", l.commitPos, len(l.entries)))
	}
	return l.entries[l.commitPos].Index
}

// lastIndex returns the index of the most recent log entry.
func (l *raftLog) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndexWithLock()
}

func (l *raftLog) lastIndexWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// 获取最后一个日志项的 term。
// lastTerm returns the term of the most recent log entry.
func (l *raftLog) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTermWithLock()
}

func (l *raftLog) lastTermWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// 追加日志项。
// appendEntry appends the passed log entry to the log. It will return an error
// if the entry's term is smaller than the log's most recent term, or if the
// entry's index is too small relative to the log's most recent entry.
func (l *raftLog) appendEntry(entry logEntry) error {
	l.Lock()
	defer l.Unlock()

	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithLock()
		// 如果待追加的日志项的 term 小于日志条目中最后一项日志的 term，返回错误。
		if entry.Term < lastTerm {
			return errTermTooSmall
		}
		lastIndex := l.lastIndexWithLock()
		// 如果 term 相同，但是待追加日志项的 index 小于日志条目中最后一项日志的 index，返回错误。
		if entry.Term == lastTerm && entry.Index <= lastIndex {
			return errIndexTooSmall
		}
	}

	// 成功追加日志项。
	l.entries = append(l.entries, entry)
	return nil
}

// 将 commitIndex 之前（包含 commitIndex）的所有日志项 commit。
// commitTo commits all log entries up to and including the passed commitIndex.
// Commit means: synchronize the log entry to persistent storage, and call the
// state machine apply function for the log entry's command.
func (l *raftLog) commitTo(commitIndex uint64) error {
	if commitIndex == 0 {
		panic("commitTo(0)")
	}

	l.Lock()
	defer l.Unlock()

	// Reject old commit indexes
	if commitIndex < l.getCommitIndexWithLock() {
		return errIndexTooSmall
	}

	// Reject new commit indexes
	if commitIndex > l.lastIndexWithLock() {
		return errIndexTooBig
	}

	// 传递的 commitIndex 必须恰好等于当前最后一项已经 committed 的日志项的 index。

	// If we've already committed to the commitIndex, great!
	if commitIndex == l.getCommitIndexWithLock() {
		return nil
	}

	// We should start committing at precisely the last commitPos + 1
	pos := l.commitPos + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}

	// 把已经 committed 日志项的 index 和参数 commitIndex 之间的日志项 commit。
	// Commit entries between our existing commit index and the passed index.
	// Remember to include the passed index.
	for {
		// Sanity checks. TODO replace with plain `for` when this is stable.
		if pos >= len(l.entries) {
			panic(fmt.Sprintf("commitTo pos=%d advanced past all log entries (%d)", pos, len(l.entries)))
		}
		if l.entries[pos].Index > commitIndex {
			panic("commitTo advanced past the desired commitIndex")
		}

		// Encode the entry to persistent storage.
		if err := l.entries[pos].encode(l.store); err != nil {
			return err
		}

		// Forward non-configuration commands to the state machine.
		// Send the responses to the waiting client, if applicable.
		// 如果不是配置类型的 Log，调用 apply 函数。
		if !l.entries[pos].isConfiguration {
			resp := l.apply(l.entries[pos].Index, l.entries[pos].Command)
			if l.entries[pos].commandResponse != nil {
				select {
				case l.entries[pos].commandResponse <- resp:
					break
				case <-time.After(maximumElectionTimeout()): // << ElectionInterval
					panic("uncoöperative command response receiver")
				}
				close(l.entries[pos].commandResponse)
				l.entries[pos].commandResponse = nil
			}
		}

		// Signal the entry has been committed, if applicable.
		if l.entries[pos].committed != nil {
			l.entries[pos].committed <- true
			close(l.entries[pos].committed)
			l.entries[pos].committed = nil
		}

		// 更新 commitPos。
		// Mark our commit position cursor.
		l.commitPos = pos

		// If that was the last one, we're done.
		if l.entries[pos].Index == commitIndex {
			break
		}
		if l.entries[pos].Index > commitIndex {
			panic(fmt.Sprintf(
				"current entry Index %d is beyond our desired commitIndex %d",
				l.entries[pos].Index,
				commitIndex,
			))
		}

		// Otherwise, advance!
		pos++
	}

	// Done.
	return nil
}

// 日志项结构体。
// logEntry is the atomic unit being managed by the distributed log. A log entry
// always has an index (monotonically increasing), a term in which the Raft
// network leader first sees the entry, and a command. The command is what gets
// executed against the node state machine when the log entry is successfully
// replicated.
type logEntry struct {
	// 日志索引
	Index           uint64        `json:"index"`
	// 日志任期
	Term            uint64        `json:"term"` // when received by leader
	// 日志内容
	Command         []byte        `json:"command,omitempty"`
	// 是否已经 committed
	committed       chan bool     `json:"-"`
	// 响应
	commandResponse chan<- []byte `json:"-"` // only non-nil on receiver's log
	// 日志类型
	isConfiguration bool          `json:"-"` // for configuration change entries
}

// 序列化日志项。
// encode serializes the log entry to the passed io.Writer.
//
// Entries are serialized in a simple binary format:
//
//		 ---------------------------------------------
//		| uint32 | uint64 | uint64 | uint32 | []byte  |
//		 ---------------------------------------------
//		| CRC    | TERM   | INDEX  | SIZE   | COMMAND |
//		 ---------------------------------------------
//
func (e *logEntry) encode(w io.Writer) error {
	if len(e.Command) <= 0 {
		return errNoCommand
	}
	if e.Index <= 0 {
		return errBadIndex
	}
	if e.Term <= 0 {
		return errBadTerm
	}

	commandSize := len(e.Command)
	buf := make([]byte, 24+commandSize)

	binary.LittleEndian.PutUint64(buf[4:12], e.Term)
	binary.LittleEndian.PutUint64(buf[12:20], e.Index)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(commandSize))

	copy(buf[24:], e.Command)

	binary.LittleEndian.PutUint32(
		buf[0:4],
		crc32.ChecksumIEEE(buf[4:]),
	)

	_, err := w.Write(buf)
	return err
}

// 反序列化日志项。
// decode deserializes one log entry from the passed io.Reader.
func (e *logEntry) decode(r io.Reader) error {
	header := make([]byte, 24)

	if _, err := r.Read(header); err != nil {
		return err
	}

	command := make([]byte, binary.LittleEndian.Uint32(header[20:24]))

	if _, err := r.Read(command); err != nil {
		return err
	}

	crc := binary.LittleEndian.Uint32(header[:4])

	check := crc32.NewIEEE()
	check.Write(header[4:])
	check.Write(command)

	if crc != check.Sum32() {
		return errInvalidChecksum
	}

	e.Term = binary.LittleEndian.Uint64(header[4:12])
	e.Index = binary.LittleEndian.Uint64(header[12:20])
	e.Command = command

	return nil
}
