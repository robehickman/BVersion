Simple HTTP File Sync (SHTTPFS) is a version control system designed for managing binary files like images, audio, and video. It follows a centralised design and has a strong emphasis on code simplicity, being inspired by the 'suckless philosophy'. It is configured with text files, intentionally has no grapgical interface, and does not use the system tray at all.


SHTTPFS offers the following benifits:

### Very low working copy overhead:

Working copies (checkouts) store only the minimum data required to detect changed files, with an overhead of only a few megabytes on a repository of thousands of files. It uses vastly less disk space than Subversion (which stores two copies of everything), and vastly less than git (which stores the whole change history).


### Partial checkouts

You can specify arbitary rules for ommitting files from a working copy, while leaving them alone on the server.


### Automatic or manual syncing:

SHTTPFS can operate in two modes, either supporting manual commit and update, or periodically checking a file system for changes and updating automatically.


### Effortless system upgrades and migration:

Server updates and migrations are effortless as all server data is stored under a single directory, with no dependency on an external SQL database. Thus moving the server only requires copying one directory tree. Versions are stored on the server in a data structure similar to git, with sqlite used for transient authentication data.

### Server side whole file deduplication:

A nice side effect of the data structure used on the server is that it inherently performs whole file de-duplication.


### Atomic commits

All commits are atomic, meaning that we cannot have a half complete commit leaving the server in an inconsistent state. Failed commits are automatically reverted before the next user commits. Commits always store the committing user, current system time in UTC and can be labeled with a commit message.


### Atomic client side file system operations through journaling. 

The client needs to store both the files in the working copy, and also a manifest of there modification times in order to detect changes. When files are downloaded from the server, if a file was added to the manifest before adding it to the file system, should the system crash in-between these two operations shttpfs would detect the file as deleted, as it is missing from the file system and would subsequently delete it from the server erroniously. In order to avoid this kind of problem client side file operations are first written to a journal and flushed to disk.

Note that this system does nothing to help you if the file system is being modified by another program simultaneously. There is no sensible way to resolve this issues at the current time because common file systems do not support snapshotting. Common version control systems work around this by assuming that you will not edit the files while doing a commit.


### Client/server architecture

SHTTPFS uses a client/server architecture with version history stored exclusively on the server. While the general trend towards decentralised version control for source code is a good thing, there are problems with using that model for large volumes of binary data. I wrote SHTTPFS mainly due to storage bloat caused by subversion storing two copies of every file in a working copy, and this problem is even worse with a decentralised versioning system. Storing history on a central server is more practical as one only needs a single large hard drive array instead of one per client.


### Conflict detection.

As it is impossible to merge binary files in a general case, SHTTPFS detects conflicts on a whole file basis. If two clients edit the same file, or if a file is deleted and edited you will be notified. Conflict resolution is then performed in the client by choosing which file to keep, and you can download changed versions for comparison or manual merging.


### Public key based authentication using ed25519 via libsodium

Client server authentication is done using public key cryptography. The server generates a cryptographically strong random sequence and sends this to the client. The client signs it with it's public key and sends this signature back to the server, which checks the signature and that the token matches the one it sent out. For encryption of the stream itself this data can be tunneled over https by proxying the SHTTPFS server process.



# Server Setup

First install using setup.py.

SHTTPFS uses public key authentication, thus the first thing you need to do is generate a new keypair. First run 'shttpfs keygen', you will be asked if you wish to encrypt the private key under a password, if you do so this will be required every time you use the client.

```
shttpfs keygen
...

Private key:
bi/WU+apHh63JH32Vzd+ldklgJ0wAICRNJnM4WemSCUARBbEyFmoKcbQzhMyHy2r5a/MZ1JdX4ITWOBUcd3EYO0=

Public key: 
RBbEyFmoKcbQzhMyHy2r5a/MZ1JdX4ITWOBUcd3EYO0=
_-------------
```

Once you have a new key pair you can set up the server. First you must create a configuration file, by default this is located in '/etc/shttpfs/server.json', if you wish to change the path of this file, you can edit 'cli_tools/shttpfs_server', the path is defined at the top of this file. The most basic configuration looks like this:

```json
{
    "repositories" : {
        "example" : {
            "path" : "/srv/file_sync/example"
        }
    },
    "users" : {
        "test" : {
            "public_key" : "RBbEyFmoKcbQzhMyHy2r5a/MZ1JdX4ITWOBUcd3EYO0=",
            "uses_repositories" : ["example"]
        }
    }
}
```

The file has two chunks, 'repositories', which specifies the repositories tracked by the system and where they are stored within the file system. The second chunk stores the users. Replace 'test' with the username you wish to use, 'then insert the public key that you generated above. Make sure that the name mentioned in 'uses repositories' matches the name of the repository or you will not be able to authenticate.


```json
"<your user name>" : {
    "public_key" : "<the public key you generated>",
    "uses_repositories" : ["<repository name>"]
}
```

Now you just create the directory and run the command 'shttpfs_server' to start the server.



# Configuring and using the client

To checkout an initial working copy from the server, just do the following command. This creates a directory having the same name as your repository.


```
shttpfs checkout http(s)://localhost:8090:/<your repository name>
```

To add files to the repository, just add them to this directory and run 'shttpfs commit'. To automatically track changes run 'shttpfs autosync'. The commands available are:


*keygen

Generates a new public and private key pair


*checkout <your domain or IP>/<your repository>

Creates a new working copy from a server


*update

Update the current working copy with any changes on the server since the last time update was run


*commit -m <optional commit message>

Commit the current additions and deletions to the server


*sync -m <optional commit message>

Run an update followed by a commit


*autosync

Periodically run an update followed by a commit. Doing this periodically means that changes affecting a group of files will be handled as a group instead of one at a time, which reduces the number of commits on the server.


*list_versions

Lists recent commits


*list_changes <commit id>

Lists the changes in a commit.


*list_files <commit id>

lists all of the files in a commit





## Ignore filters

SHTTPFS can ignore files that you wish to have in a working copy but do not wish to synchronise. Create a file .shttpfs_ignore in the root of your working copy. Within this file add ignore filters, one per line and UNIX wildcards are supported. Notice that paths match exactly, if a filter does not start with a wildcard it must begin with a slash.


```
/ignored*

```


Note that these are evaluated top to bottom so be careful with wildcards.  If a file is added to the ignore list after it has been committed previously, the next time shttpfs is run the file will be deleted on the server. Also note that the ignore file and pull ignore file (next section) will be tracked by the system. If you do not wish to track them add '/.shttpfs_ignore' and '/.shttpfs_pull_ignore' to the ignore file.



## Pull ignore filters

In addition to adding files that you do not wish to push, you can add files that you do not wish to pull from the server as well. This file is called .shttpfs_pull_ignore' and is also created in the root of a working copy. It's internal format is the same as above.

Note, if you wish to download files from the server that were previously in pull ignore, and have been removed, you need to run update -f. Normally update compares only the files which have been changed. Adding this flag performs a full comparison including unchanged files.



## Conflict resolution

When the same file in two working copies is changed simultaneously, or a change and deletion happen simultaneously to the same file the system detects this as a conflict. As shttpfs was designed to manage images and other binary files and automatic merging of these is usually impossible, conflict resolution is done on a whole file basis.

When a conflict is detected, you will be notified of this. If the conflict involves a file changed on the server you will be given the opportunity to download the changed files to compare them with the files in the working copy. Weather or not you opt to do so a conflict resolution file will be written to the .shttpfs directory.  To resolve a conflict you specify weather to resolve to the servers version of the file or the one in your working copy by deleting the opposite one from the list. Once you have done so for all items, thus all of the lists contain only one item, re-run the shttpfs client and the conflict will be resolved as described in the file.

