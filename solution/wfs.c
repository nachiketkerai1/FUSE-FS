#define FUSE_USE_VERSION 30

#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <libgen.h>
#include <stdlib.h>
#include <fuse.h>
#include <assert.h>
#include <string.h>
#include "wfs.h"

void* fs_region[MAX_DISKS];
int disk_mapping[MAX_DISKS];
int wfs_rc;
enum raid_mode raid_mode;
int num_disks;

int get_inode_via_path(char* path, struct wfs_inode** inode, int disk);
char* get_data_offset(struct wfs_inode* inode, off_t offset, int alloc, int disk);
int insert_dentry(struct wfs_inode* parent, int num, char* name, int disk);
int rm_d_entry(struct wfs_inode* inode, int inum, int disk);
int get_dentry_inode_num(char* name, struct wfs_inode* inode, int disk);
struct wfs_inode* get_inode(int num, int disk);
off_t alloc_data_block(int disk);
struct wfs_inode* alloc_inode(int disk);
void set_inode_data(struct wfs_inode* inode, mode_t mode);
void free_bitmap(uint32_t position, uint32_t* bitmap);

int get_dentry_inode_num(char* name, struct wfs_inode* inode, int disk) {
    size_t directory_sz = inode->size;
    size_t entry_sz = sizeof(struct wfs_dentry);
    if (directory_sz == 0) {
        printf("Directory is empty, no entries to search.\n");
        return -1;
    }

    off_t curr_off = 0;
    while (curr_off < (off_t)directory_sz) {
        // attempt to map the directory entry at the current offset
        struct wfs_dentry* curr_entry = (struct wfs_dentry*)get_data_offset(inode, curr_off, 0, disk);
        if (!curr_entry) {
            printf("unable to access directory entry at offset %jd\n", (intmax_t)curr_off);
            return -1;
        }

        // check if this entry matches our target name
        if (curr_entry->num != 0) {
            int name_match = strcmp(curr_entry->name, name);
            if (name_match == 0) {
                // Found the matching entry
                return curr_entry->num;
            }
        }

        curr_off += entry_sz;
    }

    printf("no directory entry named '%s' found.\n", name);
    return -1;
}

int get_inode_via_path(char* path, struct wfs_inode** inode, int disk) {
    // start from the root inode (inode number 0)
    struct wfs_inode* curr_inode = get_inode(0, disk);
    if (!curr_inode) {
        printf("Error: Unable to retrieve root inode on disk %d\n", disk);
        wfs_rc = -ENOENT;
        return -1;
    }

    // because all paths start at root, skip the leading '/'
    char* working_path = path;
    if (*working_path == '/') {
        working_path++;
    }

    // if the path is empty after skipping '/', then we know we are at the root
    if (*working_path == '\0') {
        // The requested inode is the root
        *inode = curr_inode;
        return 0;
    }

    // loop through path
    while (*working_path != '\0') {
        // find the end of the current segment
        char* start_seg = working_path;
        while (*working_path != '/' && *working_path != '\0') {
            working_path++;
        }

        if (*working_path == '/') {
            *working_path = '\0';
            working_path++;
        }

        // try to find the inode number of this entry
        int next_inode_num = get_dentry_inode_num(start_seg, curr_inode, disk);
        if (next_inode_num < 0) {
            printf("directory entry '%s' not found under current inode.\n", start_seg);
            wfs_rc = -ENOENT;
            return -1;
        }

        // retrieve the inode associated with next inode number
        struct wfs_inode* next_inode = get_inode(next_inode_num, disk);
        if (!next_inode) {
            printf("Error: Inode %d not found on disk %d\n", next_inode_num, disk);
            wfs_rc = -ENOENT;
            return -1;
        }

        curr_inode = next_inode;
    }

    // current inode corresponds to the target inode for the given path
    *inode = curr_inode;
    return 0;
}

int insert_dentry(struct wfs_inode* parent, int num, char* name, int disk) {
    size_t total_size = parent->size;
    size_t entry_size = sizeof(struct wfs_dentry);
    size_t total_entries = (total_size == 0) ? 0 : (total_size / entry_size);

    // iterate through entries looking for an empty slot
    for (size_t entries_scanned = 0; entries_scanned < total_entries; entries_scanned++) {
        off_t currentOffset = entries_scanned * entry_size;

        struct wfs_dentry* current_dentry = (struct wfs_dentry*)get_data_offset(parent, currentOffset, 0, disk);
        if (!current_dentry) {
            printf("cannot map directory entry at offset %zd\n", (ssize_t)currentOffset);
            return -1; 
        }

        // check if this entry is free 
        if (current_dentry->num == 0) {
            current_dentry->num = num;
            strncpy(current_dentry->name, name, MAX_NAME);

            if (raid_mode == RAID0) {
                // Update all inodes for RAID0
                for (int diskIndex = 0; diskIndex < num_disks; diskIndex++) {
                    struct wfs_inode* replica = get_inode(parent->num, diskIndex);
                    if (replica) {
                        replica->nlinks++;
                    }
                }
            } else {
                parent->nlinks++;
            }
            return 0; 
        }
    }

    // We must allocate a new block and place the new entry there
    size_t new_block_idx = (total_entries * entry_size) / BLOCK_SIZE; 
    off_t new_block_off = new_block_idx * BLOCK_SIZE;

    struct wfs_dentry* new_dentry = (struct wfs_dentry*)get_data_offset(parent, new_block_off, 1, disk);
    if (!new_dentry) {
        printf("unable to allocate new block for directory entry.\n");
        return -1; 
    }

    new_dentry->num = num;
    strncpy(new_dentry->name, name, MAX_NAME);

    // adjust links and size for RAID0 and others
    if (raid_mode == RAID0) {
        // for RAID0, we must ensure each disk’s inode reflects the new block
        for (int disk_idx = 0; disk_idx < num_disks; disk_idx++) {
            struct wfs_inode* inode = get_inode(parent->num, disk_idx);
            if (inode) {
                inode->nlinks++;
                // increase the size on each disk’s copy
                inode->size += BLOCK_SIZE;
            }
        }
    } else {
        // in other RAID modes, we only modify the parent’s inode
        parent->nlinks++;
        parent->size += BLOCK_SIZE;
    }

    return 0; 
}

void set_inode_data(struct wfs_inode* inode, mode_t mode) {
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);

    inode->mode = mode;
    inode->uid = getuid();
    inode->gid = getgid();
    inode->size = 0;
    inode->nlinks = 1;
    inode->atim = t.tv_sec;
    inode->mtim = t.tv_sec;
    inode->ctim = t.tv_sec;
}

int rm_d_entry(struct wfs_inode* inode, int inum, int disk) {
    size_t sz = inode->size;
    struct wfs_dentry* d_entry;

    // loop through entries
    for (off_t off = 0; off < sz; off += sizeof(struct wfs_dentry)) {
        d_entry = (struct wfs_dentry*)get_data_offset(inode, off, 0, disk);

        // set number of links to zero if we found the correct entry
        if (d_entry->num == inum) { 
            d_entry->num = 0;
            return 0;
        }
    }
    return -1; 
}

char* get_data_offset_r0(struct wfs_inode* inode, off_t file_offset, int allocate, int disk) {
    int block_index = file_offset / BLOCK_SIZE;
    int max_index = D_BLOCK + (BLOCK_SIZE / sizeof(off_t));
    off_t* blocks_array = NULL;

    if (block_index > max_index) {
        printf("invalid block index %d, exceeds %d\n", block_index, max_index);
        return NULL;
    }

    int target_disk = block_index % num_disks;

    // if the block is greater than D_BLOCK, we are dealing with indirect blocks
    if (block_index > D_BLOCK) {
        int adjusted_index = block_index - IND_BLOCK; 
        // ensure indirect block allocation if needed
        if (inode->blocks[IND_BLOCK] == 0) {
            for (int idx = 0; idx < num_disks; idx++) {
                struct wfs_inode* temp = get_inode(inode->num, idx);
                temp->blocks[IND_BLOCK] = alloc_data_block(idx);
            }
        }
        // retrieve the block array from the indirect block
        blocks_array = (off_t*)((char*)fs_region[disk] + inode->blocks[IND_BLOCK]); 
        block_index = adjusted_index;
    } else {
        // direct blocks
        blocks_array = inode->blocks;
    }

    // allocate a data block if required and not already allocated
    if (allocate && blocks_array[block_index] == 0) {
        off_t new_block = alloc_data_block(target_disk);
        // update this block on every inode copy (for RAID0, each disk gets an inode)
        for (int d = 0; d < num_disks; d++) {
            struct wfs_inode* retrieved_inode = get_inode(inode->num, d);
            if (blocks_array != inode->blocks) {
                // indirect block scenario
                off_t* indirectArr = (off_t*)((char*)fs_region[d] + retrieved_inode->blocks[IND_BLOCK]);
                
                indirectArr[block_index] = new_block;
            } else {
                // direct block scenario
                retrieved_inode->blocks[block_index] = new_block;
            }
        }
    }

    return (char*)fs_region[target_disk] + blocks_array[block_index] + (file_offset % BLOCK_SIZE);
}


char* get_data_offset_r1(struct wfs_inode* inode, off_t file_offset, int allocate, int disk) {
    int block_index = file_offset / BLOCK_SIZE;
    int max_index = D_BLOCK + (BLOCK_SIZE / sizeof(off_t));
    off_t* blocks_array = NULL;

    if (block_index > max_index) {
        printf("block index %d out of range (max %d)\n", block_index, max_index);
        return NULL;
    }

    // determine if we're dealing with indirect blocks
    if (block_index > D_BLOCK) {
        int adjusted_index = block_index - IND_BLOCK;
        // ensure the indirect block is allocated if not present
        if (inode->blocks[IND_BLOCK] == 0 && allocate) {
            inode->blocks[IND_BLOCK] = alloc_data_block(disk);
        }
        blocks_array = (off_t*)((char*)fs_region[disk] + inode->blocks[IND_BLOCK]);
        block_index = adjusted_index;
    } else {
        // direct block scenario for RAID1
        blocks_array = inode->blocks;
    }

    // allocate a block if needed
    if (allocate && blocks_array[block_index] == 0) {
        off_t new_block = alloc_data_block(disk);
        blocks_array[block_index] = new_block;
    }

    // if still zero and we needed allocation but couldn't get it, return NULL
    if (blocks_array[block_index] == 0) {
        if (allocate) {
            wfs_rc = -ENOSPC;
        }
        return NULL;
    }

    return (char*)fs_region[disk] + blocks_array[block_index] + (file_offset % BLOCK_SIZE);
}


char* get_data_offset(struct wfs_inode* inode, off_t offset, int alloc, int disk) {
    switch(raid_mode) {
        case RAID0:
            return get_data_offset_r0(inode, offset, alloc, disk);
        case RAID1:
        case RAID1v:
            return get_data_offset_r1(inode, offset, alloc, disk);
        case UNKNOWN:
            printf("unknown raid mode\n");
            return NULL;
    }
    // should never get here
    return NULL;
}

void free_bitmap(uint32_t pos, uint32_t* bitmap) {
    uint32_t region_index = pos / 32;
    uint32_t bit_off = pos % 32;

    // ensure the bitmap pointer is valid before attempting an operation
    if (!bitmap) {
        printf("null bitmap pointer, cannot release bit.\n");
        return;
    }

    // clear the bit at the given position
    uint32_t mask = ~(1 << bit_off);
    bitmap[region_index] = bitmap[region_index] & mask;
}

struct wfs_inode* get_inode(int inode_number, int disk) {
    struct wfs_sb* superblock = (struct wfs_sb*)fs_region[disk];
    if (!superblock) {
        printf("no superblock found on disk %d\n", disk);
        return NULL;
    }

    uint32_t* inode_bitmap = (uint32_t*)((char*)fs_region[disk] + superblock->i_bitmap_ptr);
    if (!inode_bitmap) {
        printf("unable to access inode bitmap on disk %d\n", disk);
        return NULL;
    }

    // compute which region of the bitmap and which bit correspond to this inode
    int region_index = inode_number / 32;
    int bit_pos = inode_number % 32;

    uint32_t region_value = inode_bitmap[region_index];
    uint32_t mask_test = (1 << bit_pos);

    // check if the inode is allocated
    int allocated = (region_value & mask_test) != 0;
    if (!allocated) {
        printf("inode %d on disk %d is not currently allocated.\n", inode_number, disk);
        return NULL;
    }

    // Compute the inode address 
    char * inode_base = (char*)fs_region[disk] + superblock->i_blocks_ptr;
    if (!inode_base) {
        printf("could not map inode base address on disk %d\n", disk);
        return NULL;
    }

    size_t inode_offset = inode_number * BLOCK_SIZE;
    struct wfs_inode* found_inode = (struct wfs_inode*)(inode_base + inode_offset);

    return found_inode;
}

off_t alloc_data_block(int disk) {
    struct wfs_sb* superblock = (struct wfs_sb*)fs_region[disk];
    if (!superblock) {
        printf("superblock is not accessible on disk %d\n", disk);
        return 0;
    }

    uint32_t* data_bitmap = (uint32_t*)((char*)fs_region[disk] + superblock->d_bitmap_ptr);

    size_t num_data_blocks = superblock->num_data_blocks;
    size_t bitmap_len = num_data_blocks / 32;
    if (!data_bitmap) {
        printf("data bitmap not accessible for disk %d\n", disk);
        return 0;
    }

    // find free data block
    off_t free_block_idx = -1;
    size_t region_index = 0;
    while (region_index < bitmap_len && free_block_idx < 0) {
        uint32_t region_value = data_bitmap[region_index];

        // if full (all 1 bits), skip 
        if (region_value == 0xFFFFFFFF) {
            region_index++;
            continue;
        }

        // check each bit in the region to find a free block
        for (uint32_t bit_pos = 0; bit_pos < 32; bit_pos++) {
            uint32_t mask = (1 << bit_pos);
            if ((region_value & mask) == 0) {
                // found a free block, mark it as allocated
                data_bitmap[region_index] |= mask;
                free_block_idx = 32 * region_index + bit_pos;
                break;
            }
        }

        region_index++;
    }

    if (free_block_idx < 0) {
        printf("cannot allocate data block on disk %d, no free blocks\n", disk);
        return 0;
    }

    return superblock->d_blocks_ptr + (BLOCK_SIZE * free_block_idx);
}


struct wfs_inode* alloc_inode(int disk) {
    struct wfs_sb* superblock = (struct wfs_sb*)fs_region[disk];
    if (!superblock) {
        printf("Error: Superblock not accessible for inode allocation on disk %d\n", disk);
        wfs_rc = -ENOSPC;
        return NULL;
    }

    uint32_t* inode_bitmap = (uint32_t*)((char*)fs_region[disk] + superblock->i_bitmap_ptr);
    size_t num_inodes = superblock->num_inodes;
    size_t inode_map_len = num_inodes / 32;

    if (!inode_bitmap) {
        printf("inode bitmap not accessible on disk %d\n", disk);
        wfs_rc = -ENOSPC;
        return NULL;
    }

    // find a free inode block 
    off_t inode_block_idx = -1;
    size_t block_region = 0;
    while (block_region < inode_map_len && inode_block_idx < 0) {
        uint32_t curr_region = inode_bitmap[block_region];
        
        // if this entire region is allocated, skip 
        if (curr_region == 0xFFFFFFFF) {
            block_region++;
            continue;
        }

        // free inode slot
        for (uint32_t bit_off = 0; bit_off < 32; bit_off++) {
            uint32_t mask_test = (1 << bit_off);
            if ((curr_region & mask_test) == 0) {
                // mark this inode as allocated
                inode_bitmap[block_region] |= mask_test;
                inode_block_idx = 32 * block_region + bit_off;
                break;
            }
        }

        block_region++;
    }

    if (inode_block_idx < 0) {
        printf("no free inodes available on disk %d\n", disk);
        wfs_rc = -ENOSPC;
        return NULL;
    }

    struct wfs_inode* new_inode = (struct wfs_inode*)((char*)fs_region[disk] + superblock->i_blocks_ptr + (BLOCK_SIZE * inode_block_idx));
    if (!new_inode) {
        printf("unable to map new inode on disk %d\n", disk);
        wfs_rc = -ENOSPC;
        return NULL;
    }

    new_inode->num = inode_block_idx;
    return new_inode;
}

int wfs_mknod(const char* path, mode_t mode, dev_t dev) {
    printf("*** mknod: %s\n", path);
    switch(raid_mode) {
        case RAID0: {
            printf("mknod raid 0\n");
            struct wfs_inode* parent_inode = NULL;
            char *base = strdup(path);
            char *name = strdup(path);

            int rc = get_inode_via_path(dirname(base), &parent_inode, 0);
            if (rc < 0) {
                return wfs_rc;
            }

            struct wfs_inode* inode = NULL;
            for (int i = 0; i < num_disks; i++) {
                inode = alloc_inode(i);
                if (inode == NULL) {
                    printf("cannot allocate inode!\n");
                    return wfs_rc;
                }
                set_inode_data(inode, S_IFREG | mode);
            }

            if (insert_dentry(parent_inode, inode->num, basename(name), 0) < 0) {
                printf("cannot add dentry!\n");
                return wfs_rc;
            }

            free(base);
            free(name);
            break;
        }
        case RAID1:
        case RAID1v:
            printf("mknod raid 1\n");
            // do for all disks because of mirroring
            for(int i = 0; i < num_disks; i++) {
                struct wfs_inode* parent_inode = NULL;
                char *base = strdup(path);
                char *name = strdup(path);

                if (get_inode_via_path(dirname(base), &parent_inode, i) < 0) {
                    return wfs_rc;
                }

                struct wfs_inode* inode = NULL;
                inode = alloc_inode(i);
                if (inode == NULL) {
                    printf("Cannot allocate inode!\n");
                    return wfs_rc;
                }
                set_inode_data(inode, S_IFREG | mode);

                if (insert_dentry(parent_inode, inode->num, basename(name), i) < 0) {
                    printf("Cannot add dentry!\n");
                    return wfs_rc;
                }

                free(base);
                free(name);
            }
            break;
        case UNKNOWN:
            printf("got uknown raid mode\n");
            return wfs_rc;
    }
    return 0; 
}

int wfs_mkdir(const char* path, mode_t mode) {
    printf("*** mkdir\n");
    switch(raid_mode) {
        case RAID0: {
            struct wfs_inode* parent_inode = NULL;
            char *base = strdup(path);
            char *name = strdup(path);

            if (get_inode_via_path(dirname(base), &parent_inode, 0) < 0) {
                return wfs_rc;
            }

            struct wfs_inode* inode = NULL;
            printf("raid 0 mkdir\n");
            for (int i = 0; i < num_disks; i++) {
                inode = alloc_inode(i);
                if (inode == NULL) {
                    printf("cannot allocate inode\n");
                    return wfs_rc;
                }
                set_inode_data(inode, S_IFDIR | mode);
            }

            if (insert_dentry(parent_inode, inode->num, basename(name), 0) < 0) {
                printf("cannot add dentry\n");
                return wfs_rc;
            }

            free(name);
            free(base);
            break;
        }
        case RAID1:
        case RAID1v:
            printf("raid 1 mkdir\n");
            for (int i = 0; i < num_disks; i++) {
                struct wfs_inode* parent_inode = NULL;
                char *base = strdup(path);
                char *name = strdup(path);

                if (get_inode_via_path(dirname(base), &parent_inode, i) < 0) {
                    return wfs_rc;
                }

                struct wfs_inode* inode = NULL;
                inode = alloc_inode(i);
                if (inode == NULL) {
                    printf("cannot allocate inode\n");
                    return wfs_rc;
                }
                set_inode_data(inode, S_IFDIR | mode);

                if (insert_dentry(parent_inode, inode->num, basename(name), i) < 0) {
                    printf("cannot add dentry\n");
                    return wfs_rc;
                }

                free(name);
                free(base);
            }
            break;
        case UNKNOWN:
            printf("got unknown raid mode\n");
    }
    return 0;
}

void set_statbuf(struct stat *statbuf, struct wfs_inode *inode) {
    statbuf->st_mode = inode->mode;
    statbuf->st_uid  = inode->uid;
    statbuf->st_gid  = inode->gid;
    statbuf->st_size = inode->size;
    statbuf->st_atime = inode->atim;
    statbuf->st_mtime = inode->mtim;
    statbuf->st_ctime = inode->ctim;
    statbuf->st_nlink = inode->nlinks;
}

int wfs_getattr(const char* path, struct stat *statbuf) {
    printf("*** getattr: %s\n", path);
    struct wfs_inode* inode;
    char* curr_path = strdup(path);
    if (!curr_path || get_inode_via_path(curr_path, &inode, 0) < 0) {
        printf("cannot get inode from path\n");
        return wfs_rc;
    }

    set_statbuf(statbuf, inode);

    free(curr_path);
    return 0;
}

int read_helper(const char* path, char *buf, size_t length, off_t offset, struct fuse_file_info *fi, int disk) {
    struct wfs_inode* inode = NULL;
    char* curr_path = strdup(path);

    if (!curr_path || get_inode_via_path(curr_path, &inode, disk) < 0) {
        printf("cannot get inode from path!\n");
        return wfs_rc;
    }
    size_t total_read = 0;
    size_t current_position = offset;

    // while we haven't read the entire lengh and the current position is less than the size of the inode
    while (total_read < length && current_position < inode->size) {
        size_t bytes_left = BLOCK_SIZE - (current_position % BLOCK_SIZE);

        size_t  bytes_remaining_in_file = inode->size - current_position;
        if (bytes_left > bytes_remaining_in_file) { 
            bytes_left = bytes_remaining_in_file; 
        }

        char* block_address = get_data_offset(inode, current_position, 0, disk);
        memcpy(buf + total_read, block_address, bytes_left);

        current_position += bytes_left;
        total_read += bytes_left;
    }

    free(curr_path);
    return total_read;
}

int wfs_read_r1v(const char* path, char *buf, size_t length, off_t offset, struct fuse_file_info *fi) {
     // Allocate and initialize checksum array
    int disk_checksums[MAX_DISKS];
    memset(disk_checksums, 0, sizeof(disk_checksums));

    // Read from all disks and compute checksums
    for (int disk_index = 0; disk_index < num_disks; ++disk_index) {
        read_helper(path, buf, length, offset, fi, disk_index);
        int checksum = 0;
        for (size_t s = 0; s < length; s++) {
            checksum += buf[s];
        }
        disk_checksums[disk_index] = checksum; 
    }

    // Count checksum occurrences
    int num_checksums[MAX_DISKS] = {0};
    for (int i = 0; i < num_disks; ++i) {
        if (disk_checksums[i] == -1) {
            continue; // Skip invalid checksums
        }

        for (int j = 0; j < num_disks; ++j) {
            if (disk_checksums[i] == disk_checksums[j]) {
                num_checksums[i]++;
            }
        }
    }

    // Identify the disk with the most common checksum
    int best_disk = -1;
    int highest_count = 0;

    for (int i = 0; i < num_disks; ++i) {
        if (num_checksums[i] > highest_count) {
            highest_count = num_checksums[i];
            best_disk = i;
        }
    }

    if (best_disk == -1) {
        printf("No valid disk found for reading.\n");
        return -1; 
    }

    // Perform the final read from the selected disk
    printf("Selected disk %d for read operation based on checksum agreement.\n", best_disk);
    return read_helper(path, buf, length, offset, fi, best_disk);
}

int wfs_read(const char* path, char *buf, size_t length, off_t offset, struct fuse_file_info *fi) {
    switch (raid_mode) {
        case RAID1v:
            return wfs_read_r1v(path, buf, length, offset, fi);
        case RAID0:
        case RAID1:
            return read_helper(path, buf, length, offset, fi, 0);
        case UNKNOWN:
            printf("got unknown raid mode\n");
            return wfs_rc;
    }
    return 0;
}

int wfs_write(const char *path, const char *buf, size_t length, off_t offset, struct fuse_file_info *fi) {
    printf("*** write\n");
    switch(raid_mode) {
        case RAID0: {
            printf("raid 0 write\n");
            struct wfs_inode* inode = NULL;
            char* curr_path = strdup(path);
            if (!curr_path || get_inode_via_path(curr_path, &inode, 0) < 0) {
                printf("failed to get inode\n");
                return wfs_rc;
            }

            size_t bytes_written = 0;
            size_t curr_pos = offset;
            ssize_t data_expansion = length > inode->size - offset ? length - (inode->size - offset) : 0;

            while (bytes_written < length) {
                size_t block_limit = BLOCK_SIZE - (curr_pos % BLOCK_SIZE);
                size_t bytes_to_write = (bytes_written + block_limit > length) ? length - bytes_written : block_limit;

                char* write_addr = get_data_offset(inode, curr_pos, 1, 0);
                if (!write_addr) {
                    printf("invalid offest\n");
                    free(curr_path);
                    return wfs_rc;
                }

                memcpy(write_addr, buf + bytes_written, bytes_to_write);
                curr_pos += bytes_to_write;
                bytes_written += bytes_to_write;
            }

            if (data_expansion > 0) {
                inode->size += data_expansion;
            }

            for (int disk_index = 0; disk_index < num_disks; ++disk_index) {
                struct wfs_inode* replica = get_inode(inode->num, disk_index);
                if (replica) {
                    replica->size = inode->size;
                }
            }

            free(curr_path);
            return bytes_written;
        }
        case RAID1:
        case RAID1v: {
            int total_bytes_written = 0;

            for (int disk_index = 0; disk_index < num_disks; ++disk_index) {
                struct wfs_inode* inode = NULL;
                char* curr_path = strdup(path);

                if (!curr_path || get_inode_via_path(curr_path, &inode, disk_index) < 0) {
                    printf("failed to retrieve inode for disk %d, path: %s\n", disk_index, path);
                    free(curr_path);
                    return wfs_rc;
                }

                size_t bytes_processed = 0;
                size_t curr_position = offset;
                ssize_t data_expansion = length > inode->size - offset ? length - (inode->size - offset) : 0;

                while (bytes_processed < length) {
                    size_t block_limit = BLOCK_SIZE - (curr_position % BLOCK_SIZE);
                    size_t bytes_to_write = (bytes_processed + block_limit > length) ? length - bytes_processed : block_limit;

                    char* write_addr = get_data_offset(inode, curr_position, 1, disk_index);
                    if (!write_addr) {
                        printf("invalid data offset for disk %d\n", disk_index);
                        free(curr_path);
                        return wfs_rc;
                    }

                    memcpy(write_addr, buf + bytes_processed, bytes_to_write);
                    curr_position += bytes_to_write;
                    bytes_processed += bytes_to_write;
                }

                if (data_expansion > 0) {
                    inode->size += data_expansion;
                }

                free(curr_path);
                total_bytes_written = bytes_processed;
            }

            return total_bytes_written;
        }
        case UNKNOWN:
            printf("got unknown raid mode\n");
            return wfs_rc;
    }

    // this shouldn't occur
    return wfs_rc;
}

int wfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi) {
    printf("Directory read request for: %s\n", path);

    // Start by adding the current and parent directory entries
    if (filler(buf, ".", NULL, 0) != 0 || filler(buf, "..", NULL, 0) != 0) {
        printf("Error adding special entries '.' and '..'\n");
        return wfs_rc;
    }

    // Attempt to resolve the inode for the given directory path
    char* curr_path = strdup(path);
    if (!curr_path) {
        printf("Error: Memory allocation for path resolution failed\n");
        return wfs_rc;
    }

    struct wfs_inode* dir_inode = NULL;
    int rc = get_inode_via_path(curr_path, &dir_inode, 0);

    if (rc < 0 || !dir_inode) {
        printf("Error: Failed to retrieve inode for path: %s\n", path);
        free(curr_path);
        return wfs_rc;
    }

    // Proceed only if directory size is valid
    size_t dir_size = dir_inode->size;
    if (dir_size == 0) {
        printf("Empty directory: %s\n", path);
        free(curr_path);
        return 0; // Success for an empty directory
    }

    // Iterate through the directory entries in a step-by-step fashion
    off_t curr_offset = 0;
    while (curr_offset < dir_size) {
        struct wfs_dentry* d_entry = (struct wfs_dentry*)get_data_offset(dir_inode, curr_offset, 0, 0);

        if (d_entry && d_entry->num != 0) {
            // Add the directory entry to the buffer
            if (filler(buf, d_entry->name, NULL, 0) != 0) {
                printf("Error adding entry: %s\n", d_entry->name);
                free(curr_path);
                return wfs_rc;
            }
        }

        curr_offset += sizeof(struct wfs_dentry);
    }

    // Cleanup and return success
    free(curr_path);
    return 0;
}

int wfs_unlink(const char* path) {
    int start_disk = 0;
    int end_disk = (raid_mode == RAID0) ? 0 : (num_disks - 1);

    // keep track if any unlink operation fails on any disk.
    int overall_status = 0;

    int current_disk = start_disk;
    while (current_disk <= end_disk) {
        printf("processing unlink on disk %d for path %s\n", current_disk, path);

        // Duplicate path strings for manipulation:
        char* parent_path = strdup(path);
        char* target_path = strdup(path);

        if (!parent_path || !target_path) {
            printf("memory allocation error during unlink.\n");
            free(parent_path);
            free(target_path);
            return wfs_rc;
        }

        char* parent_dir = dirname(parent_path);
        struct wfs_inode* parent_inode = NULL;
        int rc = get_inode_via_path(parent_dir, &parent_inode, current_disk);

        if (rc < 0 || !parent_inode) {
            printf("failed to retrieve parent inode for: %s on disk %d\n", path, current_disk);
            free(parent_path);
            free(target_path);
            overall_status = wfs_rc;
            // if RAID0, fail immediately; otherwise, continue to next disk.
            if (raid_mode == RAID0) {
                break;
            }
            current_disk++;
            continue;
        }

        // retrieve the target inode:
        struct wfs_inode* target_inode = NULL;
        int target_rc = get_inode_via_path(target_path, &target_inode, current_disk);
        if (target_rc < 0 || !target_inode) {
            printf("Failed to retrieve target inode for: %s on disk %d\n", path, current_disk);
            free(parent_path);
            free(target_path);
            overall_status = wfs_rc;
            if (raid_mode == RAID0) {
                break;
            }
            current_disk++;
            continue;
        }

        // free indirect blocks first:
        if (target_inode->blocks[IND_BLOCK] != 0) {
            off_t* indirect_blocks_array = (off_t*)((char*)fs_region[current_disk] + target_inode->blocks[IND_BLOCK]);
            size_t idx = 0;
            size_t max_index = BLOCK_SIZE / sizeof(off_t);
            while (idx < max_index) {
                if (indirect_blocks_array[idx] != 0) {
                    off_t block = indirect_blocks_array[idx];
                    struct wfs_sb* superblock = (struct wfs_sb*)fs_region[current_disk];
                    memset((char*)fs_region[current_disk] + block, 0, BLOCK_SIZE);
                    free_bitmap((block - superblock->d_blocks_ptr) / BLOCK_SIZE, 
                        (uint32_t*)((char*)fs_region[current_disk] + superblock->d_bitmap_ptr));
                }
                idx++;
            }
        }

        // free direct blocks:
        int block_index = 0;
        while (block_index < N_BLOCKS) {
            if (target_inode->blocks[block_index] != 0) {
                if (raid_mode == RAID0) {
                    // spread blocks across disks 
                    int block_disk = block_index % num_disks;
                    off_t block = target_inode->blocks[block_index];
                    struct wfs_sb* sb = (struct wfs_sb*)fs_region[block_disk];
                    memset((char*)fs_region[block_disk] + block, 0, BLOCK_SIZE);
                    free_bitmap((block - sb->d_blocks_ptr) / BLOCK_SIZE,
                        (uint32_t*)((char*)fs_region[block_disk] + sb->d_bitmap_ptr));
                } else {
                    // blocks are mirrored on each disk
                    off_t block = target_inode->blocks[block_index]; 
                    struct wfs_sb* sb = (struct wfs_sb*)fs_region[current_disk];
                    memset((char*)fs_region[current_disk] + block, 0, BLOCK_SIZE);
                    free_bitmap((block- sb->d_blocks_ptr) / BLOCK_SIZE, (uint32_t*)((char*)fs_region[current_disk] + sb->d_bitmap_ptr));
                }
            }
            block_index++;
        }

        // remove directory entry from the parent
        if (rm_d_entry(parent_inode, target_inode->num, current_disk) != 0) {
            printf("failed to remove directory entry for %s on disk %d\n", path, current_disk);
            free(parent_path);
            free(target_path);
            overall_status = wfs_rc;
            if (raid_mode == RAID0) {
                break;
            }
            current_disk++;
            continue;
        }

        if (raid_mode == RAID0) {
            int inode_num = target_inode->num;
            int disk_index = 0;
            while (disk_index < num_disks) {
                struct wfs_inode* replicated = get_inode(inode_num, disk_index);
                if (replicated) {
                    struct wfs_sb* sb = (struct wfs_sb*)fs_region[disk_index];
                    memset((char*)replicated, 0, BLOCK_SIZE); 
                    free_bitmap(((char*)replicated - ((char*)fs_region[disk_index] + sb->i_blocks_ptr)) / BLOCK_SIZE, 
                                            (uint32_t*)((char*)fs_region[disk_index] + sb->i_bitmap_ptr));
                }
                disk_index++;
            }
        } else {
            struct wfs_sb* sb = (struct wfs_sb*)fs_region[current_disk];
            memset((char*)target_inode, 0, BLOCK_SIZE); 
            free_bitmap(((char*)target_inode - ((char*)fs_region[current_disk] + sb->i_blocks_ptr)) / BLOCK_SIZE, 
                            (uint32_t*)((char*)fs_region[current_disk] + sb->i_bitmap_ptr));
        }

        free(parent_path);
        free(target_path);

        if (raid_mode == RAID0) {
            break;
        } else {
            current_disk++;
        }
    }

    if (overall_status != 0) {
        printf("unlink operation encountered an error.\n");
        return wfs_rc;
    }

    return 0;
}

static struct fuse_operations ops = {
    .getattr = wfs_getattr,
    .mknod = wfs_mknod,
    .mkdir = wfs_mkdir,
    .unlink = wfs_unlink,
    .rmdir = wfs_unlink,
    .read = wfs_read,
    .write = wfs_write,
    .readdir = wfs_readdir,
};

int validate_superblock() {
    printf("initiating superblock validation process.\n");

    // map and read the primary superblock from disk 0
    struct wfs_sb* primary_sb = (struct wfs_sb*)((char*)fs_region[0] + 0);
    if (!primary_sb) {
        printf("unable to access primary superblock!\n");
        return -1;
    }

    // initialize disk mapping for the primary superblock
    disk_mapping[primary_sb->disk_id] = 0;

    // start from the second disk and validate against the primary superblock
    int curr_disk = 1;
    while (curr_disk < num_disks) {
        printf("checking superblock consistency for disk: %d\n", curr_disk);

        struct wfs_sb* curr_sb = (struct wfs_sb*)((char*)fs_region[curr_disk] + 0);
        if (!curr_sb) {
            printf("unable to read superblock from disk %d\n", curr_disk);
            return -1;
        }

        // check for raid_mode mismatch
        if (primary_sb->raid_mode != curr_sb->raid_mode) {
            printf("mismatch in raid_mode for disk %d\n", curr_disk);
            printf("wrong superblocks\n");
            return -1;
        }

        // perform a memory-level comparison on a subset of the superblock
        int memcmp_rc = memcmp((void*)primary_sb, (void*)curr_sb, (size_t)48);
        if (memcmp_rc != 0) {
            printf("memory comparison failed for disk %d superblock\n", curr_disk);
            printf("wrong superblocks\n");
            return -1;
        }

        // if all checks pass, update disk mapping
        disk_mapping[curr_sb->disk_id] = curr_disk;

        // move on to the next disk
        curr_disk++;
    }

    return 0;
}

int main(int argc, char** argv) {
	char *disk_names[MAX_DISKS]; 
	
	// count the number of disks
	int i = 1;
	int disk_index = 0; // is the number of disks
	while (i < argc){
		if (argv[i][0] == '-'){
			// skip over the fuse flags
			break;
		}
		disk_names[disk_index] = strdup(argv[i]);
		disk_index++;
		i++;	
	}

	num_disks = disk_index;
	printf("got %d disk(s)\n", num_disks);
	int file_desc[MAX_DISKS]; 

	struct stat file_stats;
	
	// open disks and map them into memory	
	for (int j = 0; j < num_disks; j++){
		file_desc[j] = open(disk_names[j], O_RDWR, 0666);
		if (file_desc[j] < 0){
			return 1;
		}	
		
		int fstat_rc = fstat(file_desc[j], &file_stats);
		if (fstat_rc < 0){
			return 1;
		}

		// memory map all the disk images
		fs_region[j] = mmap(NULL, file_stats.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, file_desc[j], 0);
		if (fs_region[j] == NULL){
			return 1;
		}	
	}
	
	// create arguments to pass to fuse_main
	char **fuse_argv;
	fuse_argv = calloc(16, sizeof(char *));
	fuse_argv[0] = argv[0]; // want to set the fs program name
	int fuse_index = 1;
	int fuse_argc = 1;
	while (i < argc){
		fuse_argv[fuse_index] = argv[i];
		fuse_index++;
		i++;
		fuse_argc++;	
	}

	int is_valid_sb = validate_superblock();
	if (is_valid_sb != 0){
		return -1;
	}	

	struct wfs_sb *first_sb = (struct wfs_sb *) ((char *)fs_region[0]);
	raid_mode = first_sb->raid_mode;
	
	void* reorderd[10];
	for(int i = 0; i < num_disks; i++) {
		reorderd[i] = fs_region[disk_mapping[i]];
	}
	
	memcpy(fs_region, reorderd, num_disks * sizeof(void*));

	// check that the root inode exists across disks
	for(int i = 0; i < num_disks; i++) {
		struct wfs_inode *root_inode = get_inode(0, i);
		if(!root_inode) {
			return -1;
		}	
	}

	int fuse_rc = fuse_main(fuse_argc, fuse_argv, &ops, NULL);

	for(int i = 0; i < num_disks; i++) {
		munmap(fs_region[i], file_stats.st_size);
		close(file_desc[i]);
	}	
	return fuse_rc;
}
