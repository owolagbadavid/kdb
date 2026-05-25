package kdb

// SetFlushFaultForTest installs a one-shot fault hook that fires inside
// flushLocked between SSTable rename and manifest save. Used by tests
// to exercise partial-flush recovery paths. The hook is cleared after
// it fires once.
//
// This symbol is only compiled into the test binary.
func (db *DB) SetFlushFaultForTest(f func() error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.flushFault = f
}
