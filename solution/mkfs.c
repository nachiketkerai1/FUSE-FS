#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <fcntl.h>
#include <time.h>
#include <linux/stat.h>

#include "wfs.h"

// raid mode enum

enum raid_mode raid_mode_convert(char *mode) {
    if(strcmp(mode,"0") == 0) {
        return RAID0;
    } else if(strcmp(mode,"1") == 0) {
        return RAID1;
    } else if(strcmp(mode,"1v") == 0) {
        return RAID1v;
    }
    return UNKNOWN;
}

int init_superblock(struct wfs_sb *sb, int num_inodes, int num_blocks, int raid_mode, int disk_id, char **disk_images, off_t disk_size) {  
    int status = 0;

    sb->num_inodes = num_inodes;
    sb->num_data_blocks = num_blocks;
    sb->raid_mode = raid_mode;
    sb->disk_id = disk_id;
    sb->i_bitmap_ptr = sizeof(struct wfs_sb);
    sb->d_bitmap_ptr = sb->i_bitmap_ptr + (num_inodes) / 8;
    sb->i_blocks_ptr = sb->d_bitmap_ptr + (num_blocks) / 8;

    if(sb->i_blocks_ptr % BLOCK_SIZE != 0) {
        sb->i_blocks_ptr = (sb->i_blocks_ptr/ BLOCK_SIZE + 1) * BLOCK_SIZE;
    }

    sb->d_blocks_ptr = sb->i_blocks_ptr + (num_inodes * BLOCK_SIZE);

    int minimum_size = (num_blocks * BLOCK_SIZE) + sb->d_blocks_ptr;
    if(disk_size < minimum_size) {
        status = -1;
    }
    return status;
}

int main(int argc, char *argv[]) {
    int opt;

    int raid_mode = -1;
    int num_disks = 0;
    char **disk_images = malloc((num_disks + 1) * sizeof(char *));
    int num_inodes = -1;
    int num_blocks = -1;

    while ((opt = getopt(argc, argv, "r:d:i:b:")) != -1) {
        switch (opt) {
            // raid mode
            // 0, 1, 2
            case 'r':
                raid_mode = raid_mode_convert(optarg);
                break;
            // disk image name
            case 'd':
                disk_images[num_disks++] = optarg;
                disk_images = realloc(disk_images, (num_disks + 1) * sizeof(char *));
                break;
            // number of inodes
            case 'i':
                num_inodes = atoi(optarg);
                if (num_inodes < 0) {
                    fprintf(stderr, "Invalid number of inodes\n");
                    exit(EXIT_FAILURE);
                }
                num_inodes = (num_inodes + 31) & ~31;
                break;
            // number of blocks
            // must be rounded to a multiple of 32
            case 'b':
                num_blocks = atoi(optarg);
                if (num_blocks < 0) {
                    fprintf(stderr, "Invalid number of blocks\n");
                    exit(EXIT_FAILURE);
                }
                num_blocks = (num_blocks + 31) & ~31;
                break;
            default:
                fprintf(stderr, "Usage: %s [-r|-d|-i|-b] <disk image>\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
    
    if(num_disks < 2) {
        exit(1);
    }

    if(raid_mode == UNKNOWN) {
        exit(1);
    }

    for(int i = 0; i < num_disks; i++) {
        struct stat st;
        memset(&st, 0, sizeof(struct stat));
        int disk = open(disk_images[i], O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
        fstat(disk, &st);
        struct wfs_sb sb;
        int status = init_superblock(&sb, num_inodes, num_blocks, raid_mode, i, disk_images, st.st_size);
        if(status != 0) {
            exit(-1);
        }


        if(write(disk, &sb, sizeof(struct wfs_sb)) != sizeof(struct wfs_sb)) {
            fprintf(stderr, "Failed to write superblock\n");
            exit(EXIT_FAILURE);
        }

        struct wfs_inode inode;
        memset(&inode, 0, sizeof(struct wfs_inode));

        struct timespec t;
        clock_gettime(CLOCK_REALTIME, &t);

        inode.mode = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR;
        inode.uid = getuid();
        inode.gid = getgid();
        inode.size = 0;
        inode.nlinks = 1;
        inode.atim = t.tv_sec;
        inode.mtim = t.tv_sec; 
        inode.ctim = t.tv_sec;

        u_int32_t bit = 0x1;
        lseek(disk, sb.i_bitmap_ptr, SEEK_SET);
        if(write(disk, &bit, sizeof(u_int32_t)) < 0) {
            fprintf(stderr, "Failed to write inode bitmap\n");
            exit(EXIT_FAILURE);
        }
        
        lseek(disk, sb.i_blocks_ptr, SEEK_SET);
        if(write(disk, &inode, sizeof(struct wfs_inode)) < 0) {
            fprintf(stderr, "Failed to write inode\n");
            exit(EXIT_FAILURE);
        }
        close(disk);
    }
    return 0;
}
