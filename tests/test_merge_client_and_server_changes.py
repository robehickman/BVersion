from unittest import TestCase
from shttpfs.merge_client_and_server_changes import *

class TestMerge(TestCase):
############################################################################################
    def test_file_tree_merge(self):
        def item(status):
            return {'/name/file' : {
                'path'     : '/name/file',
                'status'   : status}}

        cpush = 'client_push_files'
        cpull = 'client_pull_files'
        cdel  = 'to_delete_on_client'
        sdel  = 'to_delete_on_server'
        cflt  = 'conflict_files'

        #   server            client           should be occupied, everything else should be empty
        tests = [(item('new'),     {},              cpull),
                 (item('changed'), {},              cpull),
                 (item('deleted'), {},              cdel),
                 #---
                 ({},              item('new'),     cpush),
                 ({},              item('changed'), cpush),
                 ({},              item('deleted'), sdel),
                 #---
                 (item('new'),     item('new'),     cflt),
                 (item('new'),     item('changed'), cflt),
                 (item('new'),     item('deleted'), cflt),
                 #---
                 (item('changed'), item('new'),     cflt),
                 (item('changed'), item('changed'), cflt),
                 (item('changed'), item('deleted'), cflt),
                 #---
                 (item('deleted'), item('new'),     cflt),
                 (item('deleted'), item('changed'), cflt),
                 (item('deleted'), item('deleted'), cdel)]

        # do the tests
        test_id = 0
        for test in tests:
            result = merge_client_and_server_changes(test[0], test[1]); check = result.pop(test[2])
            if any(True for k,v in result.iteritems() if v != []): raise Exception('Category occupied which should be empty. Test id ' + str(test_id))
            if len(check) != 1: raise Exception('Item should be in '+test[2]+ " but is not. Test id " + str(test_id))
            test_id += 1

