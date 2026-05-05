#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a commercial
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# test_layered106.py
#   Tests for parallel ingest-table drain (key-range subdivision).
#   Covers plan tests 2-4 and 6-7:
#     2. Multiple tables with mixed sizes drained concurrently.
#     3. Empty ingest table skipped; stable data survives step-up.
#     4. drain_threads=1 forces single-worker full-table drain.
#     6. Prepared transaction (single key) redirected correctly during drain.
#     7. Prepared transaction spanning multiple ranges handled correctly.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, Oplog
from wtscenario import make_scenarios

@disagg_test_class
class test_layered106(wttest.WiredTigerTestCase):
    conn_base_config = (
        ',create,statistics=(all),'
        'precise_checkpoint=true,'
        'preserve_prepared=true,'
    )

    disagg_storages = gen_disagg_storages('test_layered106', disagg_only=True)

    @property
    def base_config(self):
        return self.extensionsConfig() + self.conn_base_config

    def conn_config(self):
        return self.base_config + 'disaggregated=(role="leader")'

    @property
    def conn_follower_config(self):
        return self.base_config + 'disaggregated=(role="follower")'

    # -----------------------------------------------------------------------
    # Test 2: Multiple tables with mixed sizes drained concurrently.
    # With multiplier=1 all three tables are below MIN_RANGE_SIZE=1000 (no
    # subdivision).  With multiplier=10 the large table (5000 records) is
    # subdivided; the medium table (2000) sits right at the threshold.
    # -----------------------------------------------------------------------

    sizes = [
        ('small', dict(multiplier=1)),
        ('large', dict(multiplier=10)),
    ]

    resolve = [
        ('commit',   dict(do_commit=True)),
        ('rollback', dict(do_commit=False)),
    ]

    scenarios = make_scenarios(disagg_storages, sizes, resolve)

    def test_drain_multiple_tables(self):
        uri_a = 'layered:test_layered106_a'
        uri_b = 'layered:test_layered106_b'
        uri_c = 'layered:test_layered106_c'

        oplog = Oplog()
        t_a = oplog.add_uri(uri_a)
        t_b = oplog.add_uri(uri_b)
        t_c = oplog.add_uri(uri_c)

        # First batch: leader applies these entries and checkpoints.
        # With multiplier=10 the third table's second batch will be > MIN_RANGE_SIZE.
        n_a1 = 50  * self.multiplier
        n_b1 = 100 * self.multiplier
        n_c1 = 200 * self.multiplier
        oplog.insert(t_a, n_a1)
        oplog.insert(t_b, n_b1)
        oplog.insert(t_c, n_c1)

        for uri in (uri_a, uri_b, uri_c):
            self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n_a1 + n_b1 + n_c1)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Second batch: only the follower applies these (they go into the ingest).
        # With multiplier=10: uri_c gets 3000 ingest entries -> subdivided.
        n_a2 = 50  * self.multiplier
        n_b2 = 100 * self.multiplier
        n_c2 = 300 * self.multiplier
        oplog.insert(t_a, n_a2)
        oplog.insert(t_b, n_b2)
        oplog.insert(t_c, n_c2)
        total = n_a1 + n_b1 + n_c1 + n_a2 + n_b2 + n_c2

        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        for uri in (uri_a, uri_b, uri_c):
            session_follow.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, session_follow, 0, total)
        oplog.check(self, session_follow, 0, total)

        self.disagg_advance_checkpoint(conn_follow)
        oplog.check(self, session_follow, 0, total)

        # Step down leader, step up follower (drain runs on all three tables).
        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        # Reopen as follower and verify all three tables.
        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        oplog.check(self, session_follow, 0, total)

    # -----------------------------------------------------------------------
    # Test 3: Empty ingest table skipped; stable data survives step-up.
    # -----------------------------------------------------------------------

    def test_drain_empty_ingest_tables(self):
        uri = 'layered:test_layered106_empty'
        n = 100

        oplog = Oplog()
        t = oplog.add_uri(uri)
        oplog.insert(t, n)

        # Leader: write and checkpoint so records are in stable.
        self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Follower: create the table and pick up the leader checkpoint, but
        # write nothing to the follower's ingest.
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(uri, 'key_format=S,value_format=S')
        self.disagg_advance_checkpoint(conn_follow)

        # Step down leader, step up follower.
        # The ingest table is empty so drain is a no-op.
        self.conn.close('debug=(skip_checkpoint=true)')
        conn_follow.reconfigure('disaggregated=(role="leader")')

        conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        # Reopen as follower; the new-leader checkpoint contains the stable
        # data and the follower picks it up automatically on open.
        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        oplog.check(self, session_follow, 0, n)

    # -----------------------------------------------------------------------
    # Test 4: drain_threads=1 forces single-worker full-table drain.
    # Uses 10 000 records (above the MIN_RANGE_SIZE=1000 threshold), so with
    # the default 8 threads this table would be subdivided; with 1 thread it
    # gets a single full-table work item.
    # -----------------------------------------------------------------------

    def test_drain_single_thread(self):
        uri = 'layered:test_layered106_single'

        oplog = Oplog()
        t = oplog.add_uri(uri)

        # First batch on leader -- establishes last_checkpoint_timestamp so that
        # the follower's ingest entries (second batch) will pass the drain filter.
        n1 = 50 * self.multiplier
        oplog.insert(t, n1)

        self.session.create(uri, 'key_format=S,value_format=S')
        oplog.apply(self, self.session, 0, n1)
        self.conn.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        self.session.checkpoint()

        # Second batch goes into the follower's ingest.
        # With multiplier=10: 2000 ingest records exceed 2*MIN_RANGE_SIZE, so
        # the default 8 threads would subdivide -- but drain_threads=1 won't.
        n2 = 200 * self.multiplier
        oplog.insert(t, n2)
        total = n1 + n2

        # drain_threads must be set at connection-open time; it is not re-read
        # during reconfigure (parsed in the init-only section of conn_layered.c).
        single_thread_config = (
            self.extensionsConfig() + self.conn_base_config
            + 'disaggregated=(role="follower",drain_threads=1)'
        )
        conn_follow = self.wiredtiger_open('follower', single_thread_config)
        session_follow = conn_follow.open_session('')
        session_follow.create(uri, 'key_format=S,value_format=S')

        oplog.apply(self, session_follow, 0, total)
        oplog.check(self, session_follow, 0, total)

        self.disagg_advance_checkpoint(conn_follow)
        oplog.check(self, session_follow, 0, total)

        self.conn.close('debug=(skip_checkpoint=true)')

        # Step up -- drain runs with a single worker thread.
        conn_follow.reconfigure('disaggregated=(role="leader")')

        conn_follow.set_timestamp(
            f'stable_timestamp={self.timestamp_str(oplog.last_timestamp())}')
        session_follow.checkpoint()

        conn_follow.close()
        conn_follow = self.wiredtiger_open('follower', self.conn_follower_config)
        session_follow = conn_follow.open_session('')
        oplog.check(self, session_follow, 0, total)

    # -----------------------------------------------------------------------
    # Helpers for prepared-transaction tests (6 and 7).
    # Use integer keys so key ordering is numeric and range boundaries are
    # predictable.
    # -----------------------------------------------------------------------

    def _insert_range(self, session, cursor, start, stop, ts_start):
        """Insert integer keys [start, stop) each in its own transaction."""
        ts = ts_start
        for k in range(start, stop):
            session.begin_transaction()
            cursor.set_key(k)
            cursor.set_value(f'v{k}')
            cursor.insert()
            session.commit_transaction(f'commit_timestamp={self.timestamp_str(ts)}')
            ts += 1
        return ts  # next available timestamp

    def _verify_range(self, session, cursor, start, stop, ts_read, expect_present=True):
        """Assert keys [start, stop) are present (or absent) at the given read ts."""
        session.begin_transaction(f'read_timestamp={self.timestamp_str(ts_read)}')
        for k in range(start, stop):
            cursor.set_key(k)
            ret = cursor.search()
            if expect_present:
                self.assertEqual(ret, 0, f'key {k} missing')
                self.assertEqual(cursor.get_value(), f'v{k}')
            else:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND, f'key {k} unexpectedly found')
        session.rollback_transaction()

    def _verify_key(self, session, cursor, key, ts_read, expect_value):
        """Assert a single key equals expect_value (or is absent if None) at ts_read."""
        session.begin_transaction(f'read_timestamp={self.timestamp_str(ts_read)}')
        cursor.set_key(key)
        ret = cursor.search()
        if expect_value is None:
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND, f'key {key} unexpectedly found')
        else:
            self.assertEqual(ret, 0, f'key {key} missing')
            self.assertEqual(cursor.get_value(), expect_value)
        session.rollback_transaction()

    # -----------------------------------------------------------------------
    # Test 6: Prepared transaction (single key) redirected during drain.
    #
    # Timeline:
    #   ts 1..50   : leader writes keys 1..50 to stable (pre-drain baseline).
    #   ts 51      : stable_timestamp=50, checkpoint.
    #   [reconfigure to follower]
    #   ts 52..101 : follower writes keys 101..150 to ingest (committed).
    #   ts 200     : prepare_session prepares key 999 (prepare_timestamp=200).
    #   stable_timestamp=200.
    #   [reconfigure to leader -- drain runs]
    #   ts 300     : commit or rollback the prepared transaction.
    #   stable_timestamp=300, checkpoint.
    # -----------------------------------------------------------------------

    def test_drain_prepared_transaction(self):
        uri = 'layered:test_layered106_prep'
        self.session.create(uri, 'key_format=i,value_format=S')

        cursor = self.session.open_cursor(uri)

        # Leader writes keys 1..50 at timestamps 1..50.
        ts = self._insert_range(self.session, cursor, 1, 51, ts_start=1)
        # ts == 51 now
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(50)}')
        self.session.checkpoint()
        cursor.close()

        # Switch to follower role.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        # Follower writes keys 101..150 (committed) into the ingest.
        ts = self._insert_range(follower_session, follower_cursor, 101, 151, ts_start=ts)
        follower_cursor.close()

        # Prepare key 999 in a separate session (leaves prepared update in ingest).
        prepare_session = self.conn.open_session('')
        prepare_cursor = prepare_session.open_cursor(uri)
        prepare_session.begin_transaction()
        prepare_cursor.set_key(999)
        prepare_cursor.set_value('prepared_value')
        prepare_cursor.insert()
        prepare_session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(200)},'
            f'prepared_id={self.prepared_id_str(1)}')
        prepare_cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(200)}')

        # Step up -- drain runs, __layered_fix_prepared_transaction redirects
        # the prepared session's op->btree from ingest -> stable.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Resolve the prepared transaction after drain.
        if self.do_commit:
            prepare_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(300)},'
                f'durable_timestamp={self.timestamp_str(300)}')
        else:
            prepare_session.rollback_transaction(
                f'rollback_timestamp={self.timestamp_str(300)}')
        prepare_session.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(300)}')
        follower_session.checkpoint()

        # Verify.
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)

        # Keys 1..50: from original stable (always present).
        self._verify_range(read_session, read_cursor, 1, 51,
                           ts_read=50, expect_present=True)

        # Keys 101..150: drained from ingest (committed at ts 51..100).
        self._verify_range(read_session, read_cursor, 101, 151,
                           ts_read=200, expect_present=True)

        # Key 999: committed value if do_commit, absent if rollback.
        expected_999 = 'prepared_value' if self.do_commit else None
        self._verify_key(read_session, read_cursor, 999,
                         ts_read=300, expect_value=expected_999)

        read_cursor.close()
        read_session.close()

    # -----------------------------------------------------------------------
    # Test 7: Prepared transaction spanning multiple ranges during drain.
    #
    # Inserts 10 000 committed keys so the table is subdivided into multiple
    # ranges (MIN_RANGE_SIZE=1000, default 8 threads -> up to 8 ranges).
    # Three prepared keys are placed at the start, middle, and end of the
    # key space to guarantee they fall in different drain ranges.
    # -----------------------------------------------------------------------

    def test_drain_prepared_transaction_multi_range(self):
        uri = 'layered:test_layered106_prep_multi'
        self.session.create(uri, 'key_format=i,value_format=S')

        # Write one baseline key as leader to establish last_checkpoint_timestamp=1.
        # All follower writes start at ts=2 so they satisfy durable_start_ts > 1 and drain.
        baseline_cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        baseline_cursor.set_key(0)
        baseline_cursor.set_value('baseline')
        baseline_cursor.insert()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(1)}')
        baseline_cursor.close()
        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(1)}')
        self.session.checkpoint()

        # Switch to follower; all writes go into the ingest.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        # Insert 10 000 committed keys at ts 2..10001.
        # Use non-overlapping key space around the three prepared keys.
        committed_keys = list(range(1, 10001))
        # Reserve keys 500, 5000, 9500 for the prepared transaction.
        prepared_keys = {500, 5000, 9500}
        ts = 2  # start above last_checkpoint_timestamp=1 so entries drain
        for k in committed_keys:
            if k in prepared_keys:
                continue
            follower_session.begin_transaction()
            follower_cursor.set_key(k)
            follower_cursor.set_value(f'v{k}')
            follower_cursor.insert()
            follower_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(ts)}')
            ts += 1
        follower_cursor.close()

        # Prepare a transaction that updates the three spread-out keys.
        prepare_session = self.conn.open_session('')
        prepare_cursor = prepare_session.open_cursor(uri)
        prepare_session.begin_transaction()
        for k in sorted(prepared_keys):
            prepare_cursor.set_key(k)
            prepare_cursor.set_value(f'prep_{k}')
            prepare_cursor.insert()
        prepare_session.prepare_transaction(
            f'prepare_timestamp={self.timestamp_str(20000)},'
            f'prepared_id={self.prepared_id_str(1)}')
        prepare_cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(20000)}')

        # Step up -- drain subdivides the table; each range worker processes
        # its slice, and __layered_fix_prepared_transaction is called once
        # per prepared key by whichever range worker owns that key.
        self.conn.reconfigure('disaggregated=(role="leader")')

        # Resolve prepared transaction after drain.
        if self.do_commit:
            prepare_session.commit_transaction(
                f'commit_timestamp={self.timestamp_str(30000)},'
                f'durable_timestamp={self.timestamp_str(30000)}')
        else:
            prepare_session.rollback_transaction(
                f'rollback_timestamp={self.timestamp_str(30000)}')
        prepare_session.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(30000)}')
        follower_session.checkpoint()

        # Verify committed keys are all present.
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)

        read_session.begin_transaction(
            f'read_timestamp={self.timestamp_str(20000)}')
        for k in committed_keys:
            if k in prepared_keys:
                continue
            read_cursor.set_key(k)
            self.assertEqual(read_cursor.search(), 0, f'committed key {k} missing')
            self.assertEqual(read_cursor.get_value(), f'v{k}')
        read_session.rollback_transaction()

        # Verify the three prepared keys.
        for k in sorted(prepared_keys):
            expected = f'prep_{k}' if self.do_commit else None
            self._verify_key(read_session, read_cursor, k,
                             ts_read=30000, expect_value=expected)

        read_cursor.close()
        read_session.close()

    # -----------------------------------------------------------------------
    # Test 8: Standalone ingest tombstone eviction.
    #
    # A "standalone" tombstone arises when a document was inserted before oplog
    # application began on this node -- so the document's insert lives only in
    # the stable btree -- and is subsequently deleted on the follower.  The
    # ingest btree then holds a tombstone with NO backing on-disk value.
    #
    # This exercises the guard added in rec_visibility.c that allows the
    # reconciler to evict such a page without asserting "No on-disk value is
    # found".  It also verifies that after step-up and drain the delete is
    # reflected in the stable table.
    #
    # Timeline:
    #   ts=10 : leader inserts 'key_to_delete' -> stable btree
    #   stable_timestamp=10, checkpoint
    #   [reconfigure to follower]
    #   ts=20 : follower deletes 'key_to_delete' -> tombstone in ingest only
    #   ts=21 : follower inserts 'key_sentinel'  -> ingest (same page)
    #   force eviction of the ingest page          -> exercises rec_visibility.c fix
    #   [reconfigure to leader -- drain runs]
    #   verify 'key_to_delete' absent, 'key_sentinel' present
    # -----------------------------------------------------------------------

    def test_drain_standalone_ingest_tombstone(self):
        uri = 'layered:test_layered106_tombstone'
        ingest_uri = 'file:test_layered106_tombstone.wt_ingest'

        # Leader: insert key_to_delete so it lives in the stable btree only.
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        cursor.set_key('key_to_delete')
        cursor.set_value('original_value')
        cursor.insert()
        self.session.commit_transaction(f'commit_timestamp={self.timestamp_str(10)}')
        cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(10)}')
        self.session.checkpoint()

        # Reconfigure to follower -- subsequent writes go to the ingest btree.
        self.conn.reconfigure('disaggregated=(role="follower")')

        follower_session = self.conn.open_session('')
        follower_cursor = follower_session.open_cursor(uri)

        # Delete key_to_delete: tombstone lands in ingest with no backing insert
        # in the ingest btree (the only insert is in stable from above).
        follower_session.begin_transaction()
        follower_cursor.set_key('key_to_delete')
        follower_cursor.remove()
        follower_session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(20)}')

        # Insert a sentinel key so the ingest page has a non-tombstone update
        # that an eviction cursor can search for to position on the same page.
        follower_session.begin_transaction()
        follower_cursor.set_key('key_sentinel')
        follower_cursor.set_value('sentinel_value')
        follower_cursor.insert()
        follower_session.commit_transaction(
            f'commit_timestamp={self.timestamp_str(21)}')
        follower_cursor.close()

        self.conn.set_timestamp(f'stable_timestamp={self.timestamp_str(21)}')

        # Force eviction of the ingest btree page.  The page holds a tombstone
        # for key_to_delete with no on-disk backing -- this is the code path
        # guarded by the WT_URI_IS_INGEST check in rec_visibility.c.
        evict_session = self.conn.open_session('debug=(release_evict_page)')
        evict_cursor = evict_session.open_cursor(ingest_uri)
        evict_cursor.set_key('key_sentinel')
        evict_cursor.search()  # positions on the page that also holds the tombstone
        evict_cursor.close()   # triggers eviction of the page
        evict_session.close()

        # Step up -- drain copies both the tombstone and the sentinel to stable.
        self.conn.reconfigure('disaggregated=(role="leader")')

        follower_session.checkpoint()

        # Verify: key_to_delete absent (tombstone drained), key_sentinel present.
        read_session = self.conn.open_session('')
        read_cursor = read_session.open_cursor(uri)

        read_session.begin_transaction(f'read_timestamp={self.timestamp_str(21)}')

        read_cursor.set_key('key_to_delete')
        self.assertEqual(read_cursor.search(), wiredtiger.WT_NOTFOUND,
                         'key_to_delete should be absent after tombstone drain')

        read_cursor.set_key('key_sentinel')
        self.assertEqual(read_cursor.search(), 0,
                         'key_sentinel should be present after drain')
        self.assertEqual(read_cursor.get_value(), 'sentinel_value')

        read_session.rollback_transaction()
        read_cursor.close()
        read_session.close()

if __name__ == '__main__':
    wttest.run()
