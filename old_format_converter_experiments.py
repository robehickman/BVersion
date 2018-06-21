############################################################################################
# Sort deleted items with move detection
############################################################################################
def sort_deleted_items(duplicate_files, deleted_files):

    def match_helper(array, key, value):
        try: return array[key] != value
        except IndexError: return True

    def new_item(a, b):
        return {'type' : a, 'items' : b}

    processed_deleted_items = []
    for file_hash, files_with_hash in dict(duplicate_files).iteritems():
        if len(files_with_hash) > 1:
            seen_items = {}
            items = []
            for i in files_with_hash:
                items.append(i)
                seen_items[i['file_info']['path']] = None
                if i['file_info']['path'] in deleted_files:
                    items.append('deleted')

            # skip non duplicated items
            if len(seen_items) <= 1:
                continue 

            # items which do not contain a 'deleted' marker are duplicates of the same file
            elif 'deleted' not in items:
                continue 

            else:
                buf = deepcopy(items)


                while True:
                    if buf == []: break

                    if buf[0] == 'deleted':
                        raise Exception('Code logic error')

                    # handle items that are too short but would cause a false positive if checking code were reordered
                    if len(buf) <= 2:
                        # A single duplicate file, usually a double commit after an moved item in the buffer
                        if len(buf) == 1:
                            buf = []

                        # a single item has been added and then deleted
                        elif buf[0] != 'deleted' and buf[1] == 'deleted':
                            processed_deleted_items.append(new_item('single', buf[:2]))
                            buf = buf[2:]

                        # Trap to catch anything not handled above
                        else:
                            raise Exception('Unhandled pattern')

                    # item has been moved from a to b
                    elif buf[0] != buf[2] and buf[1] == 'deleted' and match_helper(buf, 3, 'deleted'):
                        processed_deleted_items.append(new_item('moved', buf[:3]))
                        buf = buf[3:]

                    # A file has been added, then a duplicate added later and subsequently removed
                    elif buf[0] != buf[1] and buf[2] == 'deleted':
                        processed_deleted_items.append(new_item('duplicate', buf[:3]))
                        buf = buf[3:]

                    # two versions of the same file exist, the duplicate has been removed
                    elif buf[0] != 'deleted' and buf[1] == 'deleted':
                        processed_deleted_items.append(new_item('single', buf[:2]))
                        buf = buf[2:]

                    # Nothing is a delete, so this is a bunch of duplicate files that still exist.
                    # Handle by removing them one at a time as later items may need different treatment
                    elif 'deleted' not in buf[:3]:
                        buf = buf[1:]

                    # Trap to catch anything not handled above
                    else:
                        raise Exception('Unhandled pattern')

    return processed_deleted_items


############################################################################################
# Inserts delete and move markers
############################################################################################
def apply_deleted_items(new_all_files, processed_deleted_items):
    def new_item(item):
        return {'file_info'           : item, 
                'global_file_id'      : None,
                'original_manifest_id': None}

    for item in processed_deleted_items:
        if item['type'] == 'moved':

            file_deleted_on = item['items'][0]['file_info']['created']
            file_created_on2 = item['items'][2]['file_info']['last_mod']

            time_delta = float(file_created_on2) - float(file_deleted_on)
            
            # File moved
            if time_delta > 0 and time_delta < 120:
                file_deleted_on = item['items'][0]['file_info']['created']

                new_all_files.append(new_item({ u'status'   : u'deleted',
                                                u'created'  : file_deleted_on,
                                                u'last_mod' : file_deleted_on,
                                                u'path'     : item['items'][0]['file_info']['path']}))

#                new_all_files.append(new_item({ u'status'   : u'moved',
 #                                               u'created'  : file_deleted_on,
  #                                              u'last_mod' : file_deleted_on,
   #                                             u'from_path'   : item['items'][0]['file_info']['path'],
    #                                            u'to_path'     : item['items'][2]['file_info']['path']}))


            # Larger time deltas are caused by removing a duplicate file by copying it over another,
            # treat this as a delete.
            else:
                file_deleted_on = item['items'][0]['file_info']['created']
                new_all_files.append(new_item({ u'status'   : u'deleted',
                                                u'created'  : file_deleted_on,
                                                u'last_mod' : file_deleted_on,
                                                u'path'     : item['items'][0]['file_info']['path']}))


        if item['type'] == 'single':
            file_created_on = item['items'][0]['file_info']['last_mod']
            file_deleted_on = item['items'][0]['file_info']['created']

            if file_deleted_on < file_created_on: raise Exception('File deletion before creation')
            if file_deleted_on == file_created_on: raise Exception('Identical creation time')

            new_all_files.append(new_item({ u'status'   : u'deleted',
                                            u'created'  : file_deleted_on,
                                            u'last_mod' : file_deleted_on,
                                            u'path'     : item['items'][0]['file_info']['path']}))


        if item['type'] == 'duplicate':
            file_created_on = item['items'][1]['file_info']['last_mod']
            file_deleted_on = item['items'][1]['file_info']['created']

            if file_deleted_on < file_created_on: raise Exception('File deletion before creation')
            if file_deleted_on == file_created_on: raise Exception('Identical creation time')

            new_all_files.append(new_item({ u'status'   : u'deleted',
                                            u'created'  : file_deleted_on,
                                            u'last_mod' : file_deleted_on,
                                            u'path'     : item['items'][1]['file_info']['path']}))


    # resort so that deleted and moved items appear in the correct place
    new_all_files = sorted(new_all_files, key=lambda x: float(x['file_info']['created']))
    return new_all_files


###########################
not_found = []
for group_id in sorted(dict(grouped_files).keys()):
    new_items = {}
    moved_items = []
    for item in grouped_files[group_id]:
        if item['file_info']['status']   == 'new':
            if item['file_info']['path'] not in new_items:
                 new_items[item['file_info']['path']] = item
            else:
                print 'Path referenced twice in one revision!'
                quit()

        elif item['file_info']['status'] == 'moved':
            moved_items.append(item)

    for moved_item in moved_items:
        if moved_item['file_info']['to_path'] in new_items:
            del new_items[moved_item['file_info']['to_path']]
            print 'moved from found'
        else:
            not_found.append((group_id, moved_item))


# an experiment being kept for the time being
#=========================================================
changes   = defaultdict(list)
unmatched = {}
dynamic_state = {}
i = 0
while True:
    manifest          = manifests[i]

    # The only manifest that should ever be able to be empty is the head. Empty
    # manifests of earlier versions should be impossible, However the time stamp
    # grouping code will break if there is an empty manifest, so putting this
    # trap in to be safe.
    if i != len(manifests) - 1 and manifest == {}:
        raise Exception("Non head manifest is empty")

    # everything in the first revision is always a newly added file
    if i == 0:
        for cur_name, cur_info in manifest.iteritems():
            changes[i].append(('new', cur_info))
            dynamic_state[cur_name] = 'changed'

        if len(manifests) == 1: break

    # Decode following revisions
    elif i < len(manifests) - 1:
        for cur_name, cur_info in manifest.iteritems():
            # If the file does not exist in any prior revision or has been deleted,
            # the file is newly added
            if cur_name not in dynamic_state or dynamic_state[cur_name] == 'delete':
                changes[i].append(('new', cur_info))
                dynamic_state[cur_name] = 'new'

            # If file exists in this and any prior revision where it has not been delieted,
            # the file has been changed 
            elif cur_name in dynamic_state and dynamic_state[cur_name] != 'delete':
                changes[i].append(('changed', cur_info))
                dynamic_state[cur_name] = 'changed'

            # If the file exists in this revision but not the following one, it was deleted
            elif cur_name not in manifests[i + 1]:
                changes[i].append(('delete', cur_info))
                dynamic_state[cur_name] = 'delete'

    # This data structure works by shifting all files in the head forward whenever
    # a file would be overwritten. Because of this the head revision contains many
    # files that where actually added in previous revisions. Separate these so they
    # can be re-inserted into the correct previous revision using timestamps. Files
    # which have been changed relative to a previous revision are probably fine.
    else:
        for cur_name, cur_info in manifest.iteritems():
            if cur_name in dynamic_state and dynamic_state[cur_name] != 'delete':
                changes[i].append(('changed', cur_info))
                dynamic_state[cur_name] = 'changed'
            else:
                unmatched[cur_info['path']] = cur_info
        break


    #-------------------------------------
    i += 1

