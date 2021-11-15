BVersion is a centralised version control system for managing binary files like images, audio, and video.

BVersion offers the following benefits:


### Very low working copy overhead:

All version history is stored on the server and working copies (checkouts) store only the minimum data required to detect changed files, with an overhead of only a few megabytes on a repository of thousands of files.


### Partial checkouts

You can specify arbitary rules for omitting files from a working copy, while leaving them alone on the server.


### Effortless system upgrades and migration:

Server updates and migrations are effortless as all server data is stored under a single directory.


### Atomic commits

All commits are atomic, it is imposable to have a cancelled commit leave the server in an inconsistent state.


### Server side whole file deduplication:

If the same  file is uploaded multiple times, only one copy will be stored on the server to reduce storage requirements.


### Conflict detection.

BVersion detects conflicts on a whole file basis. If two clients edit the same file you will be notified while running an update. Conflict resolution is then performed in the client on a whole file basis. You can download server versions for comparison or manual merging.


### Public key based authentication using ed25519 via libsodium

Client server authentication is implemented using public key cryptography.


# Installing

```
git clone https://github.com/robehickman/BVersion.git
cd BVersion
sudo pip3 install .
```

# Server Setup

BVersion uses public key authentication. the first thing you need to do is generate a new keypair. First run 'bvn keygen', you will be asked if you wish to encrypt the private key under a password, if you do so this will be required every time you use the client.

```
bvn keygen
...

Private key:
bi/WU+apHh63JH32Vzd+ldklgJ0wAICRNJnM4WemSCUARBbEyFmoKcbQzhMyHy2r5a/MZ1JdX4ITWOBUcd3EYO0=

Public key: 
RBbEyFmoKcbQzhMyHy2r5a/MZ1JdX4ITWOBUcd3EYO0=
_-------------
```

Once you have a new key pair you can set up the server. First you must create a configuration file, by default this is located in '/etc/bversion/server.json'. The most basic configuration looks like this:

```json
{
    "repositories" : {
        "example" : {
            "path" : "/srv/bversion/example"
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

Finally:

```
mkdir /srv/bversion
bvn_server
```

Now you just create the repositiory directory. To st and run the command 'bvn_server' to start the server.



# Configuring and using the client

To checkout an initial working copy from the server, just do the following command. This creates a directory having the same name as your repository.


```
bvn checkout http(s)://localhost:8090:/<your repository name>
```

To add files to the repository, just add them to this directory and run 'bvn commit'. The commands available are:


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


*list_versions

Lists recent commits


*list_changes <commit id>

Lists the changes in a commit.


*list_files <commit id>

lists all of the files in a commit





## Ignore filters

BVersion can ignore files that you wish to have in a working copy but do not wish to synchronise. Create a file .bvn_ignore in the root of your working copy. Within this file add ignore filters, one per line and UNIX wildcards are supported. Notice that paths match exactly, if a filter does not start with a wildcard it must begin with a slash.


```
/ignored*

```


Note that these are evaluated top to bottom so be careful with wildcards.  If a file is added to the ignore list after it has been committed previously, the next time BVersion is run the file will be deleted on the server. Also note that the ignore file and pull ignore file (next section) will be tracked by the system. If you do not wish to track them add '/.bvn_ignore' and '/.bvn_pull_ignore' to the ignore file.



## Pull ignore filters

In addition to adding files that you do not wish to push, you can add files that you do not wish to pull from the server as well. This file is called .bvn_pull_ignore' and is also created in the root of a working copy. It's internal format is the same as above.

Note, if you wish to download files from the server that were previously in pull ignore, and have been removed, you need to run update -f. Normally update compares only the files which have been changed. Adding this flag performs a full comparison including unchanged files.



## Conflict resolution

When the same file in two working copies is changed simultaneously, or a change and deletion happen simultaneously to the same file the system detects this as a conflict. As BVersion was designed to manage images and other binary files and automatic merging of these is usually impossible, conflict resolution is done on a whole file basis.

When a conflict is detected, you will be notified of this. If the conflict involves a file changed on the server you will be given the opportunity to download the changed files to compare them with the files in the working copy. Weather or not you opt to do so a conflict resolution file will be written to the .bvn directory.  To resolve a conflict you specify weather to resolve to the servers version of the file or the one in your working copy by deleting the opposite one from the list. Once you have done so for all items, thus all of the lists contain only one item, re-run BVersion update, and the conflict will be resolved as described in the file.

