import json, hashlib, os, os.path, shutil
from  collections import defaultdict
from datetime import datetime

import shttpfs3.common as sfs

class versioned_storage(object):
    def __init__(self, base_path):
        self.base_path = base_path
        sfs.make_dirs_if_dont_exist(sfs.cpjoin(base_path, 'index') + '/')
        sfs.make_dirs_if_dont_exist(sfs.cpjoin(base_path, 'files') + '/')


#===============================================================================
    def gc_log_item(self, item_type, item_hash):
        with open(sfs.cpjoin(self.base_path, 'gc_log'), 'a') as gc_log:
            gc_log.write(item_type + ' ' + item_hash + '\n'); gc_log.flush()


#===============================================================================
    def write_index_object(self, object_type, contents):
        new_object = {'type' : object_type}
        new_object.update(contents)
        serialised = json.dumps(new_object)
        object_hash = hashlib.sha256(bytes(serialised, encoding='utf8')).hexdigest()
        target_base = sfs.cpjoin(self.base_path, 'index',object_hash[:2])
        if os.path.isfile(sfs.cpjoin(target_base, object_hash[2:])): return object_hash

        # log items which do not exist for garbage collection
        self.gc_log_item(object_type, object_hash)

        #----
        sfs.make_dirs_if_dont_exist(target_base)
        sfs.file_put_contents(sfs.cpjoin(target_base, object_hash[2:]), bytes(serialised, encoding='utf8'))
        return object_hash


#===============================================================================
    def read_index_object(self, object_hash, expected_object_type):
        index_object = json.loads(sfs.file_get_contents(sfs.cpjoin(self.base_path, 'index', object_hash[:2], object_hash[2:])))
        if index_object['type'] != expected_object_type: raise IOError('Type of object does not match expected type')
        return index_object


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

        json_d = self.read_index_object(file_hash, 'tree')
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
    def have_active_commit(self):
        """ Checks if there is an active commit owned by the specified user """

        commit_state = sfs.file_or_default(sfs.cpjoin(self.base_path, 'active_commit'), None)
        if commit_state != None: return True
        return False


#===============================================================================
    def get_head(self):
        """ Gets the hash associated with the current head commit """

        return sfs.file_or_default(sfs.cpjoin(self.base_path, 'head'), 'root')


#===============================================================================
# NOTE Everything below here must not be called concurrently, either from
# threads in a single process or from multiple processes
#===============================================================================
    def begin(self):
        if self.have_active_commit(): raise Exception()

        active_files = {}
        head = self.get_head()
        if head != 'root':
            commit = self.read_index_object(head, 'commit')
            active_files = self.flatten_dir_tree(self.read_dir_tree(commit['tree_root']))

        # Active commit files stores all of the files which will be in this revision,
        # including ones carried over from the previous revision
        sfs.file_put_contents(sfs.cpjoin(self.base_path, 'active_commit_files'), bytes(json.dumps(active_files), encoding='utf8'))

        # Active commit changes stores a log of files which have been added, changed
        # or deleted in this revision
        sfs.file_put_contents(sfs.cpjoin(self.base_path, 'active_commit_changes'), bytes(json.dumps([]), encoding='utf8'))

        # Store that there is an active commit
        sfs.file_put_contents(sfs.cpjoin(self.base_path, 'active_commit'), b'true')


#===============================================================================
    def update_system_file(self, file_name, callback):
        contents = json.loads(sfs.file_get_contents(sfs.cpjoin(self.base_path, file_name)))
        contents = callback(contents)
        sfs.file_put_contents(sfs.cpjoin(self.base_path, file_name), bytes(json.dumps(contents), encoding='utf8'))


#===============================================================================
    def fs_put_from_file(self, source_file, file_info):
        if not self.have_active_commit(): raise Exception()
        file_info['hash'] = file_hash = sfs.hash_file(source_file)

        target_base = sfs.cpjoin(self.base_path, 'files',file_hash[:2])
        target = sfs.cpjoin(target_base, file_hash[2:])
        if not os.path.isfile(target):
            # log items which don't already exist so that we do not have to read the objects referenced in
            # all existing commits to determine if the new objects are garbage in case of a commit roll back
            self.gc_log_item('file', file_hash)

            # ---
            sfs.make_dirs_if_dont_exist(target_base)
            shutil.move(source_file, target)
        else:
            os.remove(source_file)

        #=======================================================
        # Update commit changes
        #=======================================================
        def helper(contents):
            file_info['status'] = 'changed' if file_info['path'] in contents else 'new'
            return  contents + [file_info]
        self.update_system_file('active_commit_changes', helper)

        #=======================================================
        # Update commit files
        #=======================================================
        def helper2(contents):
            contents[file_info['path']] = file_info
            return contents
        self.update_system_file('active_commit_files', helper2)


#===============================================================================
    def fs_delete(self, file_info):
        if not self.have_active_commit(): raise Exception()

        #=======================================================
        # Update commit changes
        #=======================================================
        def helper(contents):
            file_info['status'] = 'deleted'
            return  contents + [file_info]
        self.update_system_file('active_commit_changes', helper)

        #=======================================================
        # Update commit files
        #=======================================================
        def helper2(contents):
            del contents[file_info['path']]
            return contents
        self.update_system_file('active_commit_files', helper2)


#===============================================================================
    def commit(self, commit_message, commit_by, commit_datetime = None):
        if not self.have_active_commit(): raise Exception()

        current_changes = json.loads(sfs.file_get_contents(sfs.cpjoin(self.base_path, 'active_commit_changes')))
        active_files    = json.loads(sfs.file_get_contents(sfs.cpjoin(self.base_path, 'active_commit_files')))

        if current_changes == []: raise Exception('Empty commit')

        # Create and store the file tree
        tree_root   = self.write_dir_tree(self.build_dir_tree(active_files))

        # If no commit message is passed store an indication of what was changed
        if commit_message == '':
            new_item = next((change for change in current_changes if change['status'] in ['new', 'changed']), None)
            deleted_item = next((change for change in current_changes if change['status'] == 'deleted'), None)

            commit_message   = "(Generated message)\n"
            if new_item     != None: commit_message += new_item['status']     + '    ' + new_item['path'] + '\n'
            if deleted_item != None: commit_message += deleted_item['status'] + '    ' + deleted_item['path'] + '\n'
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
        os.remove(sfs.cpjoin(self.base_path, 'active_commit_changes'))
        os.remove(sfs.cpjoin(self.base_path, 'active_commit_files'))
        sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'gc_log'))
        os.remove(sfs.cpjoin(self.base_path, 'active_commit'))

        return commit_object_hash


#===============================================================================
    def rollback(self):
        if not self.have_active_commit(): raise Exception()

        gc_log_contents = sfs.file_or_default(sfs.cpjoin(self.base_path, 'gc_log'), '')
        gc_log_items = [file_row.split(' ') for file_row in gc_log_contents.splitlines()]

        if gc_log_items != []:
            # If a commit exists and it's hash matches the current head we do not need to do anything
            # The commit succeeded but we failed before deleting the active commit file for some reason
            is_commit = next((item for item in gc_log_items if item[0] == 'commit'), None)
            if is_commit != None and is_commit[1] == self.get_head():
                pass # commit actually ok

            else:# commit not ok
                for item in gc_log_items:
                    # delete the object for this file, noting that it may not exist
                    print(item)
                    object_dir = 'files' if item[0] == 'file' else 'index'
                    target_base = sfs.cpjoin(self.base_path, object_dir, item[1][:2])
                    sfs.ignore(os.remove, sfs.cpjoin(target_base, item[1][2:]))
                    sfs.ignore(os.rmdir, target_base)

        sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'active_commit_changes'))
        sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'active_commit_files'))
        sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'gc_log'))
        os.remove(sfs.cpjoin(self.base_path, 'active_commit')) # if this is being called, this file should always exist


#===============================================================================
    def get_changes_since(self, version_id, head):
        pointer = head
        if pointer == version_id: return {}

        change_logs = []
        seen_pointers = {}

        while True:
            if pointer in seen_pointers: raise Exception("Cycle detected")
            commit = self.read_index_object(pointer, 'commit')
            if pointer == version_id: break
            change_logs.append(commit)
            if commit['parent'] == version_id: break
            seen_pointers[pointer] = None
            pointer = commit['parent']

        return {change['path'] : change for change_log in reversed(change_logs) for change in change_log['changes']}


#===============================================================================
    def get_commit_chain(self):
        pointer = self.get_head()
        if pointer == 'root': return []

        commits = []; limiter = 0
        while True:
            # store the id of this commit, not it's parent
            commit = self.read_index_object(pointer, 'commit')
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
    def get_commit_changes(self, version_id):
        commit = self.read_index_object(version_id, 'commit')
        return commit['changes']


#===============================================================================
    def get_commit_files(self, version_id):
        commit = self.read_index_object(version_id, 'commit')
        return self.flatten_dir_tree(self.read_dir_tree(commit['tree_root']))


#===============================================================================
    def get_file_info_from_path(self, file_path):
        head = self.get_head()
        if head == 'root': raise IOError('There are no commits!')
        tree_root = self.read_index_object(head, 'commit')['tree_root']

        def helper(tree_root, path):
            tree_contents = self.read_index_object(tree_root, 'tree')

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
    def get_file_directory_path(self, file_hash):
        return sfs.cpjoin(self.base_path, 'files', file_hash[:2])

