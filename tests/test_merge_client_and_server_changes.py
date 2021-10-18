from unittest import TestCase
from shttpfs3.merge_client_and_server_changes import merge_client_and_server_changes

class TestMerge(TestCase):
############################################################################################
    def test_file_tree_merge(self):
        def item(status):
            return {'/name/file' : {
                'path'     : '/name/file',
                'status'   : status}}

        cpush  = 'client_push_files'
        cpull  = 'client_pull_files'
        cdel   = 'to_delete_on_client'
        sdel   = 'to_delete_on_server'
        cflt   = 'conflict_files'
        error  = 'error_files'
        nop    = 'no_op'

        #        server              client             should be occupied, everything else should be empty
        tests = [(item('new'),       {},                cpull),
                 (item('changed'),   {},                cpull),
                 (item('deleted'),   {},                cdel),
                 (item('unchanged'), {},                cpull), # This would happen if the client has pull ingored
                                                                # a file, and removed it from pull ignore after
                                                                # updating past the revision where it was last
                                                                # created or changed on the server.
                 #---
                 ({},                item('new'),       cpush),
                 ({},                item('changed'),   cpush),
                 ({},                item('deleted'),   sdel),
                 ({},                item('unchanged'), error), # This should never happen, indicates that the file
                                                                # has been deleted on the server, but the client has
                                                                # somehow managed to update without that file being
                                                                # removed from the client manifest.
                 #---
                 (item('new'),       item('new'),       cflt),
                 (item('new'),       item('changed'),   cflt),
                 (item('new'),       item('deleted'),   cflt),
                 (item('new'),       item('unchanged'), cpull),
                 #---
                 (item('changed'),   item('new'),       cflt),
                 (item('changed'),   item('changed'),   cflt),
                 (item('changed'),   item('deleted'),   cflt),
                 (item('changed'),   item('unchanged'), cpull),
                 #---
                 (item('deleted'),   item('new'),       cflt),
                 (item('deleted'),   item('changed'),   cflt),
                 (item('deleted'),   item('deleted'),   cdel),
                 (item('deleted'),   item('unchanged'), cdel),
                 #---
                 (item('unchanged'), item('new'),       cpush),
                 (item('unchanged'), item('changed'),   cpush),
                 (item('unchanged'), item('deleted'),   sdel),
                 (item('unchanged'), item('unchanged'), nop)]

        # do the tests
        test_id = 0
        for test in tests:
            result = merge_client_and_server_changes(test[0], test[1]); check = result.pop(test[2])
            if any(True for k,v in result.items() if v != []): raise Exception('Category occupied which should be empty. Test id ' + str(test_id))
            if len(check) != 1: raise Exception('Item should be in '+test[2]+ " but is not. Test id " + str(test_id))
            test_id += 1
