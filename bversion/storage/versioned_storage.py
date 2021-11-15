import json, hashlib, os, os.path, shutil
from  collections import defaultdict
from datetime import datetime

from typing import List, Dict, Any, cast
from typing_extensions import TypedDict

import bversion.common as sfs
from bversion.storage.server_db import get_server_db_instance_for_thread

import pprint

#+++++++++++++++++++++++++++++++++
class indexObject(TypedDict):
    type: str

#+++++++++++++++++++++++++++++++++
class indexObjectTree(indexObject):
    files: List[Dict[str, str]]
    dirs:  List[Dict[str, str]]

#+++++++++++++++++++++++++++++++++
class indexObjectCommit(indexObject):
    parent:         str
    utc_date_time:  int
    commit_by:      str
    commit_message: str
    tree_root:      Any
    changes:        Any


#+++++++++++++++++++++++++++++++++
#+++++++++++++++++++++++++++++++++
class versioned_storage:
    def __init__(self, base_path: str):
        self.base_path = base_path
        sfs.make_dirs_if_dont_exist(sfs.cpjoin(base_path, 'index') + '/')
        sfs.make_dirs_if_dont_exist(sfs.cpjoin(base_path, 'files') + '/')


#===============================================================================
    def write_index_object(self, object_type: str, contents: Dict[str, Any]) -> str:
        new_object: indexObject = {'type' : object_type}
        new_object.update(contents) #type: ignore
        serialised = json.dumps(new_object)
        object_hash = hashlib.sha256(bytes(serialised, encoding='utf8')).hexdigest()
        target_base = sfs.cpjoin(self.base_path, 'index',object_hash[:2])

        # Does an object with this hash already exist?
        if os.path.isfile(sfs.cpjoin(target_base, object_hash[2:])):
            return object_hash

        # log items which do not exist for garbage collection
        sdb = get_server_db_instance_for_thread(self.base_path)
        sdb.gc_log_item(object_type, object_hash)

        #----
        sfs.make_dirs_if_dont_exist(target_base)
        # TODO make write and move
        sfs.file_put_contents(sfs.cpjoin(target_base, object_hash[2:]), bytes(serialised, encoding='utf8'))
        return object_hash


#===============================================================================
    def read_index_object(self, object_hash: str, expected_object_type: str) -> indexObject:
        # Hashes must only contain hex digits
        if not set(object_hash) <= set('0123456789abcdef'):
            raise IOError('Invalid object hash')

        # ==============
        index_object: indexObject = json.loads(sfs.file_get_contents(sfs.cpjoin(self.base_path, 'index', object_hash[:2], object_hash[2:])))
        if index_object['type'] != expected_object_type: raise IOError('Type of object does not match expected type')
        return index_object


#===============================================================================
    def read_tree_index_object(self, object_hash) -> indexObjectTree:
        return cast(indexObjectTree, self.read_index_object(object_hash, 'tree'))


#===============================================================================
    def read_commit_index_object(self, object_hash) -> indexObjectCommit:
        return cast(indexObjectCommit, self.read_index_object(object_hash, 'commit'))


#===============================================================================
    def build_dir_tree(self, files):
        """ Convert a flat file dict into the tree format used for storage """

        def helper(split_files):
            this_dir = {'files' : {}, 'dirs' : {}}
            dirs = defaultdict(list)

            for fle in split_files:
                index = fle[0]; fileinfo = fle[1]
                if len(index)  == 1:
                    fileinfo['path'] = index[0] # store only the file name instead of the whole path
                    this_dir['files'][fileinfo['path']] = fileinfo
                elif len(index) > 1:
                    dirs[index[0]].append((index[1:], fileinfo))

            for name,info in dirs.items():
                this_dir['dirs'][name] = helper(info)
            return this_dir
        return helper([(name.split('/')[1:], file_info) for name, file_info in files.items()])


#===============================================================================
    def flatten_dir_tree(self, tree):
        """ Convert a file tree back into a flat dict """

        result = {}

        def helper(tree, leading_path = ''):
            dirs  = tree['dirs']; files = tree['files']
            for name, file_info in files.items():
                file_info['path'] = leading_path + '/'  + name
                result[file_info['path']] = file_info

            for name, contents in dirs.items():
                helper(contents, leading_path +'/'+ name)
        helper(tree); return result


#===============================================================================
    def print_dir_tree(self, tree, indent = ''):
        dirs  = tree['dirs']; files = tree['files']
        for name in list(files.keys()): print(indent + name)
        for name, contents in dirs.items():
            print(indent + name + '/')
            self.print_dir_tree(contents, indent + '---')


#===============================================================================
    def read_dir_tree(self, file_hash):
        """ Recursively read the directory structure beginning at hash """

        json_d = self.read_tree_index_object(file_hash)
        node = {'files' : json_d['files'], 'dirs' : {}}
        for name, hsh in json_d['dirs'].items(): node['dirs'][name] = self.read_dir_tree(hsh)
        return node


#===============================================================================
    def write_dir_tree(self, tree):
        """ Recur through dir tree data structure and write it as a set of objects """

        dirs  = tree['dirs']; files = tree['files']
        child_dirs = {name : self.write_dir_tree(contents) for name, contents in dirs.items()}
        return self.write_index_object('tree', {'files' : files, 'dirs': child_dirs})


#===============================================================================
    def have_active_commit(self) -> bool:
        """ Checks if there is an active commit owned by the specified user """

        sdb = get_server_db_instance_for_thread(self.base_path)
        return sdb.have_active_commit()


#===============================================================================
    def get_head(self) -> str:
        """ Gets the hash associated with the current head commit """

        contents = sfs.file_or_default(sfs.cpjoin(self.base_path, 'head'), b'root')
        return contents.decode('utf8')


#===============================================================================
# NOTE Everything below here must not be called concurrently, either from
# threads in a single process or from multiple processes
#===============================================================================
    def begin(self) -> None:
        if self.have_active_commit(): raise Exception()

        # Possable optimisations

        # Don't store GC log in DB
        # Don't flush GC log
        # Don't store active files in DB, don't update active files dynamically, but do it in one go during commit

        active_files = {}
        head = self.get_head()

        if head != 'root':
            commit = self.read_commit_index_object(head)
            active_files = self.flatten_dir_tree(self.read_dir_tree(commit['tree_root']))

        sdb = get_server_db_instance_for_thread(self.base_path)
        sdb.begin_commit(active_files)


#===============================================================================
    def fs_put_from_file(self, source_file: str, file_info) -> None:
        if not self.have_active_commit(): raise Exception()
        file_info['hash'] = file_hash = sfs.hash_file(source_file)

        sdb = get_server_db_instance_for_thread(self.base_path)

        target_base = sfs.cpjoin(self.base_path, 'files',file_hash[:2])
        target = sfs.cpjoin(target_base, file_hash[2:])
        if not os.path.isfile(target):
            # log items which don't already exist so that we do not have to read the objects referenced in
            # all existing commits to determine if the new objects are garbage in case of a commit roll back
            sdb.gc_log_item('file', file_hash)

            # ---
            sfs.make_dirs_if_dont_exist(target_base)
            shutil.move(source_file, target)
        else:
            os.remove(source_file)

        # Update commit changes
        sdb.add_to_commit(file_info)

        return file_info


#===============================================================================
    def fs_delete(self, file_path) -> None:
        if not self.have_active_commit(): raise Exception()

        # As we always store all history, simply removing the file from the manifest
        # of this commit is all we need to do
        sdb = get_server_db_instance_for_thread(self.base_path)
        sdb.remove_from_commit(file_path)


#===============================================================================
    def commit(self, commit_message, commit_by, commit_datetime = None) -> str:
        if not self.have_active_commit(): raise Exception()

        sdb = get_server_db_instance_for_thread(self.base_path)
        current_changes, active_files = sdb.get_commit_state()

        if current_changes == []: raise Exception('Empty commit')

        # Create and store the file tree
        tree_root   = self.write_dir_tree(self.build_dir_tree(active_files))

        # If no commit message is passed store an indication of what was changed
        if commit_message == '':
            new_item = next((change for change in current_changes if change['status'] in ['new', 'changed']), None)
            deleted_item = next((change for change in current_changes if change['status'] == 'deleted'), None)

            commit_message   = "(Generated message)\n"
            if new_item     is not None: commit_message += new_item['status']     + '    ' + new_item['path'] + '\n'
            if deleted_item is not None: commit_message += deleted_item['status'] + '    ' + deleted_item['path'] + '\n'
            if len(current_changes) > 2: commit_message += '...'

        # Commit timestamp
        commit_datetime = datetime.utcnow() if commit_datetime is None else commit_datetime
        commit_timestamp = commit_datetime.strftime("%d-%m-%Y %H:%M:%S:%f")

        # Create commit
        commit_object_hash = self.write_index_object('commit', {'parent'         : self.get_head(),
                                                                'utc_date_time'  : commit_timestamp,
                                                                'commit_by'      : commit_by,
                                                                'commit_message' : commit_message,
                                                                'tree_root'      : tree_root,
                                                                'changes'        : current_changes})

        #update head, write plus move for atomicity
        sfs.file_put_contents(sfs.cpjoin(self.base_path, 'new_head'), bytes(commit_object_hash, encoding='utf8'))
        os.rename(sfs.cpjoin(self.base_path, 'new_head'), sfs.cpjoin(self.base_path, 'head'))

        #and clean up working state
        sdb.clean_commit_state()
        sdb.con.commit()

        return commit_object_hash


#===============================================================================
    def rollback(self) -> None:
        if not self.have_active_commit(): raise Exception()

        sdb = get_server_db_instance_for_thread(self.base_path)
        gc_log_items = sdb.get_gc_log()
        
        if gc_log_items != []:
            # If a commit exists and it's hash matches the current head we do not need to do anything
            # The commit succeeded but we failed before deleting the active commit file for some reason
            is_commit = next((item for item in gc_log_items if item['item_type'] == 'commit'), None)
            if is_commit is not None and is_commit['item_hash'] == self.get_head():
                pass # commit actually ok

            else:# commit not ok, need to clean up
                for item in gc_log_items:
                    # delete the object for this file, noting that it may not exist
                    object_dir = 'files' if item['item_type'] == 'file' else 'index'
                    target_base = sfs.cpjoin(self.base_path, object_dir, item['item_hash'][:2])
                    sfs.ignore(os.remove, sfs.cpjoin(target_base, item['item_hash'][2:]))
                    sfs.ignore(os.rmdir, target_base)

        sdb.clean_commit_state()
        sdb.con.commit()


#===============================================================================
    def get_changes_since(self, version_id: str, head: str):
        pointer = head
        if pointer == version_id: return {}

        change_logs = []
        seen_pointers: Dict[str, None] = {}

        while True:
            if pointer in seen_pointers: raise Exception("Cycle detected")
            commit = self.read_commit_index_object(pointer)
            if pointer == version_id: break
            change_logs.append(commit)
            if commit['parent'] == version_id: break
            seen_pointers[pointer] = None
            pointer = commit['parent']

        return {change['path'] : change for change_log in reversed(change_logs) for change in change_log['changes']}


#===============================================================================
    def get_commit_chain(self, commit_limit = 50):
        pointer = self.get_head()
        if pointer == 'root': return []

        commits = []; limiter = 0
        while True:
            # store the id of this commit, not it's parent
            commit = self.read_commit_index_object(pointer)
            commits.append({'id'             : pointer,
                            'utc_date_time'  : commit['utc_date_time'],
                            'commit_by'      : commit['commit_by'],
                            'commit_message' : commit['commit_message']})

            #--
            pointer = commit['parent']
            if pointer == 'root': break
            if limiter > 50: break
            limiter += 1
        return commits


#===============================================================================
    def get_commit_changes(self, version_id: str):
        commit = self.read_commit_index_object(version_id)
        return commit['changes']


#===============================================================================
    def get_commit_files(self, version_id: str):
        commit = self.read_commit_index_object(version_id)
        return self.flatten_dir_tree(self.read_dir_tree(commit['tree_root']))


#===============================================================================
    def get_file_info_from_path(self, file_path: str, version_id = None):

        if version_id is None:
            version_id = self.get_head()
            if version_id == 'root': raise IOError('There are no commits!')

        # ========================
        tree_root = self.read_commit_index_object(version_id)['tree_root']

        def helper(tree_root, path):
            tree_contents = self.read_tree_index_object(tree_root)

            if len(path) > 1:
                if path[0] not in tree_contents['dirs']: raise IOError('No such file or directory')
                return helper(tree_contents['dirs'][path[0]], path[1:])
            elif len(path) == 1:
                if path[0] not in tree_contents['files']: raise IOError('No such file or directory')
                return tree_contents['files'][path[0]]
            else:
                raise IOError('No such file or directory')

        split_path = file_path.split('/')
        split_path = split_path[1:] if split_path[0] == '' else split_path
        result = helper(tree_root, split_path)
        result['path'] = file_path
        return result


#===============================================================================
    def get_file_directory_path(self, file_hash: str) -> str:
        return sfs.cpjoin(self.base_path, 'files', file_hash[:2])


#===============================================================================
    def get_reachable_objects(self):
        """ Gets all objects in the filesystem which can be reached by traversing the index """

        reachable_objects = {
            'commits' : {},
            'trees'   : {},
            'files'   : {}
        }

        # TODO this needs to be able to handle missing objects and log them as errors

        head = self.get_head()
        if head == 'root':
            return head, reachable_objects

        # walk up the commit change to the first version
        pointer = head
        while True:
            commit = self.read_commit_index_object(pointer)
            reachable_objects['commits'][pointer] = commit
            pointer = commit['parent']
            if pointer == 'root': break

        # Read tree objects
        def recursive_helper(tree_object_hash):
            nonlocal reachable_objects

            it = self.read_tree_index_object(tree_object_hash)
            reachable_objects['trees'][tree_object_hash] = it

            for dir_name, dir_hash in it['dirs'].items():
                recursive_helper(dir_hash)

        for commit_info in reachable_objects['commits'].values():
            recursive_helper(commit_info['tree_root'])

        # Get file object referances
        for tree_object_info in reachable_objects['trees'].values():
            for fle in tree_object_info['files'].values():
                reachable_objects['files'][fle['hash']] = fle

        return head, reachable_objects


#===============================================================================
    def get_all_object_hashes(self):
        objects = {
            'index' : {},
            'files' : {}
        }

        # Enumerate index objects
        for it in os.listdir(sfs.cpjoin(self.base_path, 'index')):
            for it2 in os.listdir(sfs.cpjoin(self.base_path, 'index', it)):
                objects['index'][it + it2] = sfs.cpjoin(self.base_path, 'index', it, it2)

        # Enumerate file objects
        for it in os.listdir(sfs.cpjoin(self.base_path, 'files')):
            for it2 in os.listdir(sfs.cpjoin(self.base_path, 'files', it)):
                objects['files'][it + it2] = sfs.cpjoin(self.base_path, 'files', it, it2)

        return objects

#===============================================================================
    def verify_fs(self) -> str:
        """
        Checks index structure is intact, and reads and rehashes the entire
        filesystem to ensure than hashes have not changed.
        """

        head, reachable_objects = self.get_reachable_objects()
        all_objects             = self.get_all_object_hashes()

        issues = []

        #================================================================
        # if head is root, but commits exists, this is suspisious 
        #================================================================
        number_of_commits = 0
        for object_hash in all_objects['index']:
            object_path = sfs.cpjoin(self.base_path, 'index', object_hash[:2], object_hash[2:])
            file_contents = json.loads(sfs.file_get_contents(object_path))
            if file_contents['type'] == 'commit':
                number_of_commits += 1

        if head == 'root' and number_of_commits != 0:
            msg = 'Commits exist, but head is root.'
            print(msg)
            issues.append(msg)
            return False, issues

        #================================================================
        # Subtract the two, to find if there are unreachable objects
        #================================================================
        # index objects
        index_objects_that_should_exist =   set(reachable_objects['commits'].keys()) \
                                          | set(reachable_objects['trees'])

        index_objects_that_do_exist     =   set(all_objects['index'].keys())

        # file objects
        file_objects_that_should_exist =   set(reachable_objects['files'].keys())
        file_objects_that_do_exist     =   set(all_objects['files'].keys())

        # ===============
        garbage_index_objects = index_objects_that_should_exist - index_objects_that_do_exist 
        garbage_file_objects = file_objects_that_should_exist - file_objects_that_do_exist 

        # ===============
        for it in garbage_index_objects:
            msg = 'Garbage index object: ' + it
            print(msg)
            issues.append(msg)

        for it in garbage_file_objects:
            msg = 'Garbage file object: ' + it
            print(msg)
            issues.append(msg)

        #================================================================
        # Re hash all objects to check that the hashes have not changed
        #================================================================
        for object_hash, object_path in all_objects['index'].items():
            print('Rehashing: ' + object_path)
            current_hash = sfs.hash_file(object_path)
            if object_hash != current_hash:
                msg = 'Index opject hash has changed: ' + object_hash
                print(msg)
                issues.append(msg)

        for object_hash, object_path in all_objects['files'].items():
            print('Rehashing: ' + object_path)
            current_hash = sfs.hash_file(object_path)
            if object_hash != current_hash:
                msg = 'File object hash has changed: ' + object_hash
                print(msg)
                issues.append(msg)

        # ================================
        return len(issues) == 0, issues

#===============================================================================
    def garbage_collect(self):
        # TODO this needs to lock the repo for safety

        head, reachable_objects = self.get_reachable_objects()
        all_objects             = self.get_all_object_hashes()

        issues = []

        #================================================================
        # if head is root, but commits exists, this is suspisious 
        #================================================================
        number_of_commits = 0
        for object_hash in all_objects['index']:
            object_path = sfs.cpjoin(self.base_path, 'index', object_hash[:2], object_hash[2:])
            file_contents = json.loads(sfs.file_get_contents(object_path))
            if file_contents['type'] == 'commit':
                number_of_commits += 1

        if head == 'root' and number_of_commits != 0:
            msg = 'Commits exist, but head is root.'
            print(msg)
            issues.append(msg)
            return False, issues

        #================================================================
        # Subtract the two, to find if there are unreachable objects
        #================================================================
        # index objects
        index_objects_that_should_exist =   set(reachable_objects['commits'].keys()) \
                                          | set(reachable_objects['trees'])

        index_objects_that_do_exist     =   set(all_objects['index'].keys())

        # file objects
        file_objects_that_should_exist =   set(reachable_objects['files'].keys())
        file_objects_that_do_exist     =   set(all_objects['files'].keys())

        # ===============
        garbage_index_objects = index_objects_that_should_exist - index_objects_that_do_exist 
        garbage_file_objects = file_objects_that_should_exist - file_objects_that_do_exist 

        #================================================================
        # Delete garbage objects
        #================================================================
        for object_hash in garbage_index_objects:
            object_path = sfs.cpjoin(self.base_path, 'index', object_hash[:2], object_hash[2:])
            os.remove(object_path)

        for object_hash in garbage_file_objects:
            object_path = sfs.cpjoin(self.base_path, 'index', object_hash[:2], object_hash[2:])
            os.remove(object_path)


