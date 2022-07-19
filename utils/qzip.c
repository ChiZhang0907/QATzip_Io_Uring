/***************************************************************************
 *
 *   BSD LICENSE
 *
 *   Copyright(c) 2007-2021 Intel Corporation. All rights reserved.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 ***************************************************************************/
#include "qzip.h"

char const *const g_license_msg[] = {
    "Copyright (C) 2021 Intel Corporation.",
    0
};

char *g_program_name = NULL; /* program name */
int g_decompress = 0;        /* g_decompress (-d) */
int g_keep = 0;                     /* keep (don't delete) input files */
int g_io_uring = 1;
int g_speed = 0;
int g_o_direct = 1;
int g_read_o_direct = 1;
int g_write_o_direct =1;
QzSession_T g_sess;
QzSessionParams_T g_params_th = {(QzHuffmanHdr_T)0,};
struct io_uring ring;

/* Estimate maximum data expansion after decompression */
const unsigned int g_bufsz_expansion_ratio[] = {5, 20, 50, 100};

/* Command line options*/
char const g_short_opts[] = "A:H:L:C:S:r:o:O:P:dfhkVRiaD";
const struct option g_long_opts[] = {
    /* { name  has_arg  *flag  val } */
    {"decompress", 0, 0, 'd'}, /* decompress */
    {"uncompress", 0, 0, 'd'}, /* decompress */
    {"force",      0, 0, 'f'}, /* force overwrite of output file */
    {"help",       0, 0, 'h'}, /* give help */
    {"keep",       0, 0, 'k'}, /* keep (don't delete) input files */
    {"io_uring",   0, 0, 'i'}, /* don't use io_uring to read and write files */
    {"no_direct",  0, 0, 'D'}, /* don't use O_DIRECT flag to read and write files */
    {"version",    0, 0, 'V'}, /* display version number */
    {"available",  0, 0, 'a'}, /* check the hardware environment */
    {"algorithm",  1, 0, 'A'}, /* set algorithm type */
    {"huffmanhdr", 1, 0, 'H'}, /* set huffman header type */
    {"level",      1, 0, 'L'}, /* set compression level */
    {"chunksz",    1, 0, 'C'}, /* set chunk size */
    {"speed",      1, 0, 'S'}, /* set compression speed limitaion */
    {"output",     1, 0, 'O'}, /* set output header format(gzip, gzipext, 7z)*/
    {"recursive",  0, 0, 'R'}, /* set recursive mode when compressing a
                                  directory */
    {"polling",    1, 0, 'P'}, /* set polling mode when compressing and
                                  decompressing */
    { 0, 0, 0, 0 }
};

const unsigned int USDM_ALLOC_MAX_SZ = (2 * 1024 * 1024 - 5 * 1024);


void tryHelp(void)
{
    QZ_PRINT("Try `%s --help' for more information.\n", g_program_name);
    exit(ERROR);
}

void help(void)
{
    static char const *const help_msg[] = {
        "Compress or uncompress FILEs (by default, compress FILES in-place).",
        "",
        "Mandatory arguments to long options are mandatory for short options "
        "too.",
        "",
        "  -A, --algorithm   set algorithm type",
        "  -d, --decompress  decompress",
        "  -f, --force       force overwrite of output file and compress links",
        "  -h, --help        give this help",
        "  -H, --huffmanhdr  set huffman header type",
        "  -k, --keep        keep (don't delete) input files",
        "  -i, --io_uring    don't use io_uring to read and write files",
        "  -D, --no_direct   don't use O_DIRECT to read and write files",
        "  -a  --available   check the hardware environment",
        "  -V, --version     display version number",
        "  -L, --level       set compression level",
        "  -C, --chunksz     set chunk size",
        "  -S  --speed       set compression speed limitation",
        "  -O, --output      set output header format(gzip|gzipext|7z)",
        "  -r,               set max inflight request number",
        "  -R,               set Recursive mode for a directory",
        "  -o,               set output file name",
        "  -P, --polling     set polling mode, only supports busy polling settings",
        "",
        "With no FILE, read standard input.",
        0
    };
    char const *const *p = help_msg;

    QZ_PRINT("Usage: %s [OPTION]... [FILE]...\n", g_program_name);
    while (*p) {
        QZ_PRINT("%s\n", *p++);
    }
}

void freeTimeList(RunTimeList_T *time_list)
{
    RunTimeList_T *time_node = time_list;
    RunTimeList_T *pre_time_node = NULL;

    while (time_node) {
        pre_time_node = time_node;
        time_node = time_node->next;
        free(pre_time_node);
    }
}

void displayStats(RunTimeList_T *time_list,
                  off_t insize, off_t outsize, int is_compress)
{
    /* Calculate time taken (from begin to end) in micro seconds */
    unsigned long us_begin = 0;
    unsigned long us_end = 0;
    double us_diff = 0;
    RunTimeList_T *time_node = time_list;

    while (time_node) {
        us_begin = time_node->time_s.tv_sec * 1000000 +
                   time_node->time_s.tv_usec;
        us_end = time_node->time_e.tv_sec * 1000000 + time_node->time_e.tv_usec;
        us_diff += (us_end - us_begin);
        time_node = time_node->next;
    }

    if (insize) {
        if (0 == us_diff) {
            QZ_ERROR("The size do not change afteer compression\n");
            exit(QZ7Z_ERR_INVALID_SIZE);
        }
        double size = (is_compress) ? insize : outsize;
        double throughput = (size * CHAR_BIT) / us_diff; /* in MB (megabytes) */
        double compressionRatio = ((double)insize) / ((double)outsize);
        double spaceSavings = 1 - ((double)outsize) / ((double)insize);

        QZ_PRINT("Time taken:    %9.3lf ms\n", us_diff / 1000);
        QZ_PRINT("Throughput:    %9.3lf Mbit/s\n", throughput);
        if (is_compress) {
            QZ_PRINT("Space Savings: %9.3lf %%\n", spaceSavings * 100.0);
            QZ_PRINT("Compression ratio: %.3lf : 1\n", compressionRatio);
        }
    }
}

int doProcessBuffer(QzSession_T *sess,
                    unsigned char *src, unsigned int *src_len,
                    unsigned char *dst, unsigned int dst_len,
                    RunTimeList_T *time_list, FILE *dst_file,
                    off_t *dst_file_size, int is_compress)
{
    int ret = QZ_FAIL;
    unsigned int done = 0;
    unsigned int buf_processed = 0;
    unsigned int buf_remaining = *src_len;
    unsigned int bytes_written = 0;
    unsigned int valid_dst_buf_len = dst_len;
    RunTimeList_T *time_node = time_list;


    while (time_node->next) {
        time_node = time_node->next;
    }

    while (!done) {
        RunTimeList_T *run_time = calloc(1, sizeof(RunTimeList_T));
        if (NULL == run_time) {
            QZ_ERROR("Malloc run_time error.\n");
            exit(QZ7Z_ERR_MALLOC);
        }
        run_time->next = NULL;
        time_node->next = run_time;
        time_node = run_time;

        gettimeofday(&run_time->time_s, NULL);

        /* Do actual work */
        if (is_compress) {
            ret = qzCompress(sess, src, src_len, dst, &dst_len, 1);
            if (QZ_BUF_ERROR == ret && 0 == *src_len) {
                done = 1;
            }
        } else {
            ret = qzDecompress(sess, src, src_len, dst, &dst_len);

            if (QZ_DATA_ERROR == ret ||
                (QZ_BUF_ERROR == ret && 0 == *src_len)) {
                done = 1;
            }
        }

        if (ret != QZ_OK &&
            ret != QZ_BUF_ERROR &&
            ret != QZ_DATA_ERROR) {
            const char *op = (is_compress) ? "Compression" : "Decompression";
            QZ_ERROR("doProcessBuffer:%s failed with error: %d\n", op, ret);
            break;
        }

        gettimeofday(&run_time->time_e, NULL);

        bytes_written = fwrite(dst, 1, dst_len, dst_file);
        if (bytes_written != dst_len) {
            QZ_ERROR("Fwrite write less bytes than expected.\n");
            exit(QZ7Z_ERR_WRITE_LESS);
        }
        *dst_file_size += bytes_written;

        buf_processed += *src_len;
        buf_remaining -= *src_len;
        if (0 == buf_remaining) {
            done = 1;
        }
        src += *src_len;
        QZ_DEBUG("src_len is %u ,buf_remaining is %u\n", *src_len,
                 buf_remaining);
        *src_len = buf_remaining;
        dst_len = valid_dst_buf_len;
        bytes_written = 0;
    }

    *src_len = buf_processed;
    return ret;
}

int doProcessBufferDirect(QzSession_T *sess,
                    unsigned char *src, unsigned int *src_len,
                    unsigned char *dst, unsigned int dst_len,
                    RunTimeList_T *time_list, IoUringFile_T *dst_file,
                    off_t *dst_file_size, int is_compress,
                    DestBuffer_T* dest_buffer)
{
    int ret = QZ_FAIL;
    unsigned int done = 0;
    unsigned int buf_processed = 0;
    unsigned int buf_remaining = *src_len;
    ssize_t bytes_written = 0;
    unsigned int valid_dst_buf_len = dst_len;
    RunTimeList_T *time_node = time_list;


    while (time_node->next) {
        time_node = time_node->next;
    }

    while (!done) {
        RunTimeList_T *run_time = calloc(1, sizeof(RunTimeList_T));
        if (NULL == run_time) {
            QZ_ERROR("Malloc run_time error.\n");
            exit(QZ7Z_ERR_MALLOC);
        }
        run_time->next = NULL;
        time_node->next = run_time;
        time_node = run_time;

        gettimeofday(&run_time->time_s, NULL);

        /* Do actual work */
        if (is_compress) {
            ret = qzCompress(sess, src, src_len, dst, &dst_len, 1);
            if (QZ_BUF_ERROR == ret && 0 == *src_len) {
                done = 1;
            }
        } else {
            ret = qzDecompress(sess, src, src_len, dst, &dst_len);

            if (QZ_DATA_ERROR == ret ||
                (QZ_BUF_ERROR == ret && 0 == *src_len)) {
                done = 1;
            }
        }

        if (ret != QZ_OK &&
            ret != QZ_BUF_ERROR &&
            ret != QZ_DATA_ERROR) {
            const char *op = (is_compress) ? "Compression" : "Decompression";
            QZ_ERROR("doProcessBuffer:%s failed with error: %d\n", op, ret);
            break;
        }

        gettimeofday(&run_time->time_e, NULL);
        unsigned int remain_len = dst_len;
        unsigned char *dst_buffer_head = dst;
        while(remain_len > 0) {
            unsigned int buffer_reamin_size = dest_buffer->size - dest_buffer->off;
            unsigned int copy_size = 0;
            if (buffer_reamin_size > remain_len) {
                copy_size = remain_len;
            } else {
                copy_size = buffer_reamin_size;
            }
            memcpy(dest_buffer->buffer + dest_buffer->off, dst_buffer_head, copy_size);
            dst_buffer_head = dst_buffer_head + copy_size;
            dest_buffer->off += copy_size;
            remain_len -= copy_size;
            if (dest_buffer->off == dest_buffer->size) {
                bytes_written = pwrite(dst_file->fd, dest_buffer->buffer, dest_buffer->size, dst_file->off);
                if (bytes_written != dest_buffer->size) {
                    QZ_ERROR("Fwrite write less bytes than expected.\n");
                    exit(QZ7Z_ERR_WRITE_LESS);
                }
                *dst_file_size += bytes_written;
                dst_file->off += bytes_written;
                dest_buffer->off = 0;
            }
        }

        buf_processed += *src_len;
        buf_remaining -= *src_len;
        if (0 == buf_remaining) {
            done = 1;
        }
        src += *src_len;
        QZ_DEBUG("src_len is %u ,buf_remaining is %u\n", *src_len,
                 buf_remaining);
        *src_len = buf_remaining;
        dst_len = valid_dst_buf_len;
        bytes_written = 0;
    }

    *src_len = buf_processed;
    return ret;
}

int doProcessBufferIoUringDirect(QzSession_T *sess,
                    unsigned char *src, unsigned int *src_len,
                    unsigned char *dst, unsigned int dst_len,
                    RunTimeList_T *time_list, IoUringFile_T *dst_file,
                    off_t *dst_file_size, int is_compress, struct io_uring *ring_,
                    DestBuffer_T *dest_buffer)
{
    int ret = QZ_FAIL;
    unsigned int done = 0;
    unsigned int buf_processed = 0;
    unsigned int buf_remaining = *src_len;
    ssize_t bytes_written = 0;
    unsigned int valid_dst_buf_len = dst_len;
    RunTimeList_T *time_node = time_list;
    struct io_uring_sqe *sqe = NULL;


    while (time_node->next) {
        time_node = time_node->next;
    }

    while (!done) {
        RunTimeList_T *run_time = calloc(1, sizeof(RunTimeList_T));
        if (NULL == run_time) {
            QZ_ERROR("Malloc run_rime error\n");
            exit(QZ7Z_ERR_MALLOC);
        }
        run_time->next = NULL;
        time_node->next = run_time;
        time_node = run_time;

        gettimeofday(&run_time->time_s, NULL);

        /* Do actual work */
        if (is_compress) {
            ret = qzCompress(sess, src, src_len, dst, &dst_len, 1);
            if (QZ_BUF_ERROR == ret && 0 == *src_len) {
                done = 1;
            }
        } else {
            ret = qzDecompress(sess, src, src_len, dst, &dst_len);

            if (QZ_DATA_ERROR == ret ||
                (QZ_BUF_ERROR == ret && 0 == *src_len)) {
                done = 1;
            }
        }

        if (ret != QZ_OK &&
            ret != QZ_BUF_ERROR &&
            ret != QZ_DATA_ERROR) {
            const char *op = (is_compress) ? "Compression" : "Decompression";
            QZ_ERROR("doProcessBuffer:%s failed with error: %d\n", op, ret);
            break;
        }

        gettimeofday(&run_time->time_e, NULL);

        unsigned int remain_len = dst_len;
        unsigned char *dst_buffer_head = dst;
        while(remain_len > 0) {
            unsigned int buffer_reamin_size = dest_buffer->size - dest_buffer->off;
            unsigned int copy_size = 0;
            if (buffer_reamin_size > remain_len) {
                copy_size = remain_len;
            } else {
                copy_size = buffer_reamin_size;
            }
            memcpy(dest_buffer->buffer + dest_buffer->off, dst_buffer_head, copy_size);
            dst_buffer_head = dst_buffer_head + copy_size;
            dest_buffer->off += copy_size;
            remain_len -= copy_size;
            if (dest_buffer->off == dest_buffer->size) {
                sqe = io_uring_get_sqe(ring_);
                CHECK_GET_SQE(sqe)
                io_uring_prep_write(sqe, dst_file->fd, dest_buffer->buffer, dest_buffer->size, dst_file->off);
                bytes_written = getIoUringResult(ring_);
                CHECK_IO_URING_WRITE_RETURN(bytes_written, dest_buffer->size, "compress")
                *dst_file_size += bytes_written;
                dst_file->off += bytes_written;
                dest_buffer->off = 0;
            }
        }

        buf_processed += *src_len;
        buf_remaining -= *src_len;
        if (0 == buf_remaining) {
            done = 1;
        }
        src += *src_len;
        QZ_DEBUG("src_len is %u ,buf_remaining is %u\n", *src_len,
                 buf_remaining);
        *src_len = buf_remaining;
        dst_len = valid_dst_buf_len;
        bytes_written = 0;
    }

    *src_len = buf_processed;
    return ret;
}

void doProcessFile(QzSession_T *sess, const char *src_file_name,
                   const char *dst_file_name, int is_compress)
{
    int ret = OK;
    struct stat src_file_stat;
    struct timeval speed_time;
    struct timeval speed_time_tmp;
    off_t speed_size = 0, speed_limitation = g_speed * 1024 * 1024;
    useconds_t speed_val = 0;
    unsigned int src_buffer_size = 0;
    unsigned int dst_buffer_size = 0;
    unsigned int dest_buffer_size = 0;
    off_t src_file_size = 0, dst_file_size = 0, file_remaining = 0;
    unsigned char *src_buffer = NULL;
    unsigned char *dst_buffer = NULL;
    IoUringFile_T *src_file = NULL;
    IoUringFile_T *dst_file = NULL;
    DestBuffer_T* dest_buffer = NULL;
    unsigned int bytes_read = 0;
    unsigned long bytes_processed = 0;
    unsigned int ratio_idx = 0;
    const unsigned int ratio_limit =
        sizeof(g_bufsz_expansion_ratio) / sizeof(unsigned int);
    unsigned int read_more = 0;
    int src_fd = 0;
    RunTimeList_T *time_list_head = malloc(sizeof(RunTimeList_T));
    if (NULL == time_list_head) {
        QZ_ERROR("Malloc time_list error\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    gettimeofday(&time_list_head->time_s, NULL);
    time_list_head->time_e = time_list_head->time_s;
    time_list_head->next = NULL;

    ret = stat(src_file_name, &src_file_stat);
    if (ret) {
        perror(src_file_name);
        exit(ERROR);
    }

    if (S_ISBLK(src_file_stat.st_mode)) {
        if ((src_fd = open(src_file_name, O_RDONLY)) < 0) {
            perror(src_file_name);
            exit(ERROR);
        } else {
            if (ioctl(src_fd, BLKGETSIZE, &src_file_size) < 0) {
                close(src_fd);
                perror(src_file_name);
                exit(ERROR);
            }
            src_file_size *= 512;
            /* size get via BLKGETSIZE is divided by 512 */
            close(src_fd);
        }
    } else {
        src_file_size = src_file_stat.st_size;
    }
    src_buffer_size = (src_file_size > SRC_BUFF_LEN) ?
                      SRC_BUFF_LEN : src_file_size;
    
    if (src_buffer_size % 4096 != 0) {
        src_buffer_size = 4096 - (src_buffer_size % 4096) + src_buffer_size;
    }

    if (is_compress) {
        dst_buffer_size = qzMaxCompressedLength(src_buffer_size, sess);
    } else { /* decompress */
        dst_buffer_size = src_buffer_size *
                          g_bufsz_expansion_ratio[ratio_idx++];
    }

    if (0 == src_file_size && is_compress) {
        dst_buffer_size = 1024;
    }

    dest_buffer_size = (dst_buffer_size > DST_BUFF_LEN) ? DST_BUFF_LEN : dst_buffer_size;
    if (dest_buffer_size % 4096 != 0) {
        dest_buffer_size = 4096 - (dest_buffer_size % 4096) + dest_buffer_size;
    }

    src_buffer = aligned_alloc(4096, src_buffer_size);
    if (src_buffer == NULL) {
        QZ_ERROR("Malloc src_buffer error\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    dst_buffer = malloc(dst_buffer_size);
    if (dst_buffer == NULL) {
        QZ_ERROR("Malloc dst_buffer error\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    dest_buffer = generateDestBuffer(dest_buffer_size);
    if (dest_buffer == NULL) {
        QZ_ERROR("Malloc dest_buffer error\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    if (g_read_o_direct) {
        src_file = generateIoUringFile(src_file_name, O_RDONLY | O_DIRECT);
    } else {
        src_file = generateIoUringFile(src_file_name, O_RDONLY);
    }
    if (src_file == NULL) {
        QZ_ERROR("Cannot open file: %s.\n", src_file_name);
        ret = QZ7Z_ERR_OPEN;
        goto exit;
    }

    if (g_o_direct) {
        dst_file = generateIoUringFileWithMode(dst_file_name, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    } else {
        dst_file = generateIoUringFileWithMode(dst_file_name, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    }

    if(!dst_file) {
        QZ_ERROR("Cannot open file: %s\n", dst_file_name);
        ret = QZ7Z_ERR_OPEN;
        goto exit;
    }

    file_remaining = src_file_size;
    read_more = 1;
    
    if(g_speed > 0) {
        gettimeofday(&speed_time, NULL);
    }
    
    do {
        if(g_speed > 0) {
            gettimeofday(&speed_time_tmp, NULL);
            speed_val =  (speed_time_tmp.tv_sec * 1000000 + speed_time_tmp.tv_usec) - (speed_time.tv_sec * 1000000 + speed_time.tv_usec);
            if(speed_val > 1000000) {
                speed_time.tv_sec = speed_time_tmp.tv_sec;
                speed_time.tv_usec = speed_time_tmp.tv_usec;
                speed_size = 0;
            } else if (speed_size >= speed_limitation) {
                useconds_t sleep_time = 1000000 - speed_val;
                usleep(sleep_time);
                gettimeofday(&speed_time, NULL);
                speed_size = 0;
            }
        }

        if (read_more) {
            ssize_t bytes_read_temp = 0;
            bytes_read_temp = pread(src_file->fd, src_buffer, src_buffer_size, src_file->off);
            if (bytes_read_temp <= 0) {
                bytes_read = 0;
            } else {
                bytes_read =bytes_read_temp;
            }
            //QZ_PRINT("Reading input file %s (%u Bytes)\n", src_file_name,
            //         bytes_read);
            src_file->off += bytes_read;
        } else {
            bytes_read = file_remaining;
        }

        //puts((is_compress) ? "Compressing..." : "Decompressing...");

        ret = doProcessBufferDirect(sess, src_buffer, &bytes_read, dst_buffer,
                              dst_buffer_size, time_list_head, dst_file,
                              &dst_file_size, is_compress, dest_buffer);

        if (QZ_DATA_ERROR == ret || QZ_BUF_ERROR == ret) {
            bytes_processed += bytes_read;
            if (0 != bytes_read) {
                src_file->off = bytes_processed;
                read_more = 1;
            } else if (QZ_BUF_ERROR == ret) {
                //dest buffer not long enough
                if (ratio_limit == ratio_idx) {
                    QZ_ERROR("Could not expand more destination buffer\n");
                    ret = ERROR;
                    goto exit;
                }

                free(dst_buffer);
                dst_buffer_size = src_buffer_size *
                                  g_bufsz_expansion_ratio[ratio_idx++];
                dst_buffer = malloc(dst_buffer_size);
                if (NULL == dst_buffer) {
                    QZ_ERROR("Fail to allocate destination buffer with size "
                             "%u\n", dst_buffer_size);
                    ret = ERROR;
                    goto exit;
                }

                read_more = 0;
            } else {
                // corrupt data
                ret = ERROR;
                goto exit;
            }
        } else if (QZ_OK != ret) {
            QZ_ERROR("Process file error: %d\n", ret);
            ret = ERROR;
            goto exit;
        } else {
            read_more = 1;
        }

        file_remaining -= bytes_read;
        speed_size += bytes_read;

        if (g_read_o_direct && bytes_read % 4096 != 0) {
            fcntl(src_file->fd, F_SETFL, O_RDONLY);
            g_read_o_direct = 0;
        }
    } while (file_remaining > 0);

    if (dest_buffer->off != 0) {
        if (g_write_o_direct && dest_buffer->off % 4096 != 0) {
            fcntl(dst_file->fd, F_SETFL, O_WRONLY | O_CREAT | O_TRUNC);
            g_write_o_direct = 0;
        }
        int bytes_written = pwrite(dst_file->fd, dest_buffer->buffer, dest_buffer->off, dst_file->off);
        if (bytes_written != dest_buffer->off) {
            QZ_ERROR("pwrite write less bytes than expected.\n");
            exit(QZ7Z_ERR_WRITE_LESS);
        }
        dst_file_size += bytes_written;
        dst_file->off += bytes_written;
        dest_buffer->off = 0;
    }

    displayStats(time_list_head, src_file_size, dst_file_size, is_compress);

exit:
    if (time_list_head) {
        freeTimeList(time_list_head);
    }
    if (src_file) {
        freeIoUringFile(src_file);
    }
    if (dst_file) {
        freeIoUringFile(dst_file);
    }
    if (src_buffer) {
        free(src_buffer);
    }
    if (dst_buffer) {
        free(dst_buffer);
    }
    if (dest_buffer) {
        freeDestBuffer(dest_buffer);
    }
    if (!g_keep && OK == ret) {
        unlink(src_file_name);
    }
    if (ret) {
        exit(ret);
    }
}

void doProcessFileIoUring(QzSession_T *sess, const char *src_file_name,
                   const char *dst_file_name, int is_compress, struct io_uring *ring_)
{
    int ret = OK;
    struct stat src_file_stat;
    struct timeval speed_time;
    struct timeval speed_time_tmp;
    off_t speed_size = 0, speed_limitation = g_speed * 1024 * 1024;
    useconds_t speed_val = 0;
    unsigned int src_buffer_size = 0;
    unsigned int dst_buffer_size = 0;
    unsigned int dest_buffer_size = 0;
    off_t src_file_size = 0, dst_file_size = 0, file_remaining = 0;
    unsigned char *src_buffer = NULL;
    unsigned char *dst_buffer = NULL;
    DestBuffer_T *dest_buffer = NULL;
    IoUringFile_T *src_file = NULL;
    IoUringFile_T *dst_file = NULL;
    struct io_uring_sqe *sqe = NULL;
    unsigned int bytes_read = 0;
    unsigned long bytes_processed = 0;
    unsigned int ratio_idx = 0;
    const unsigned int ratio_limit =
        sizeof(g_bufsz_expansion_ratio) / sizeof(unsigned int);
    unsigned int read_more = 0;
    int src_fd = 0;
    RunTimeList_T *time_list_head = malloc(sizeof(RunTimeList_T));
    if (NULL == time_list_head) {
        QZ_ERROR("Malloc time_list error.\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    gettimeofday(&time_list_head->time_s, NULL);
    time_list_head->time_e = time_list_head->time_s;
    time_list_head->next = NULL;

    ret = stat(src_file_name, &src_file_stat);
    if (ret) {
        perror(src_file_name);
        exit(ERROR);
    }

    if (S_ISBLK(src_file_stat.st_mode)) {
        if ((src_fd = open(src_file_name, O_RDONLY)) < 0) {
            perror(src_file_name);
            exit(ERROR);
        } else {
            if (ioctl(src_fd, BLKGETSIZE, &src_file_size) < 0) {
                close(src_fd);
                perror(src_file_name);
                exit(ERROR);
            }
            src_file_size *= 512;
            /* size get via BLKGETSIZE is divided by 512 */
            close(src_fd);
        }
    } else {
        src_file_size = src_file_stat.st_size;
    }
    src_buffer_size = (src_file_size > SRC_BUFF_LEN) ?
                      SRC_BUFF_LEN : src_file_size;
    if (src_buffer_size % 4096 != 0) {
        src_buffer_size = 4096 - (src_buffer_size % 4096) + src_buffer_size;
    }
    if (is_compress) {
        dst_buffer_size = qzMaxCompressedLength(src_buffer_size, sess);
    } else { /* decompress */
        dst_buffer_size = src_buffer_size *
                          g_bufsz_expansion_ratio[ratio_idx++];
    }

    if (0 == src_file_size && is_compress) {
        dst_buffer_size = 1024;
    }

    dest_buffer_size = (dst_buffer_size > DST_BUFF_LEN) ? DST_BUFF_LEN : dst_buffer_size;
    if (dest_buffer_size % 4096 != 0) {
        dest_buffer_size = 4096 - (dest_buffer_size % 4096) + dest_buffer_size;
    }

    src_buffer = aligned_alloc(4096, src_buffer_size);
    if (src_buffer == NULL) {
        QZ_ERROR("Malloc src_buffer error.\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    dst_buffer = malloc(dst_buffer_size);
    if (dst_buffer == NULL) {
        QZ_ERROR("Malloc dst_buffer error.\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    dest_buffer = generateDestBuffer(dest_buffer_size);
    if (dest_buffer == NULL) {
        QZ_ERROR("Malloc dest_buffer errro\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    if (g_o_direct) {
        src_file = generateIoUringFile(src_file_name, O_RDONLY | O_DIRECT);
    } else {
        src_file = generateIoUringFile(src_file_name, O_RDONLY);
    }
    if(!src_file) {
        QZ_ERROR("Cannot open file: %s\n", src_file_name);
        ret = QZ7Z_ERR_OPEN;
        goto exit;
    }
    if (g_o_direct) {
        dst_file = generateIoUringFileWithMode(dst_file_name, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    } else {
        dst_file = generateIoUringFileWithMode(dst_file_name, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
    }
    if(!dst_file) {
        QZ_ERROR("Cannot open file: %s\n", dst_file_name);
        ret = QZ7Z_ERR_OPEN;
        goto exit;
    }

    file_remaining = src_file_size;
    read_more = 1;

    if(g_speed > 0) {
        gettimeofday(&speed_time, NULL);
    }

    do {
        if(g_speed > 0) {
            gettimeofday(&speed_time_tmp, NULL);
            speed_val =  (speed_time_tmp.tv_sec * 1000000 + speed_time_tmp.tv_usec) - (speed_time.tv_sec * 1000000 + speed_time.tv_usec);
            if(speed_val > 1000000) {
                speed_time.tv_sec = speed_time_tmp.tv_sec;
                speed_time.tv_usec = speed_time_tmp.tv_usec;
                speed_size = 0;
            } else if (speed_size >= speed_limitation) {
                useconds_t sleep_time = 1000000 - speed_val;
                usleep(sleep_time);
                gettimeofday(&speed_time, NULL);
                speed_size = 0;
            }
        }

        if (read_more) {
            ssize_t bytes_read_temp = 0;
            sqe = io_uring_get_sqe(ring_);
            CHECK_GET_SQE(sqe)
            io_uring_prep_read(sqe, src_file->fd, src_buffer, src_buffer_size, src_file->off);
            bytes_read_temp = getIoUringResult(ring_);
            if(bytes_read_temp <= 0) {
                bytes_read = 0;
            } else {
                bytes_read = bytes_read_temp;
            }
            //QZ_PRINT("Reading input file %s (%u Bytes)\n", src_file_name,
            //         bytes_read);
            src_file->off += bytes_read;
        } else {
            bytes_read = file_remaining;
        }

        //puts((is_compress) ? "Compressing..." : "Decompressing...");

        ret = doProcessBufferIoUringDirect(sess, src_buffer, &bytes_read, dst_buffer,
                              dst_buffer_size, time_list_head, dst_file,
                              &dst_file_size, is_compress, ring_, dest_buffer);

        if (QZ_DATA_ERROR == ret || QZ_BUF_ERROR == ret) {
            bytes_processed += bytes_read;
            if (0 != bytes_read) {
                src_file->off = bytes_processed;
                read_more = 1;
            } else if (QZ_BUF_ERROR == ret) {
                //dest buffer not long enough
                if (ratio_limit == ratio_idx) {
                    QZ_ERROR("Could not expand more destination buffer\n");
                    ret = ERROR;
                    goto exit;
                }

                free(dst_buffer);
                dst_buffer_size = src_buffer_size *
                                  g_bufsz_expansion_ratio[ratio_idx++];
                dst_buffer = malloc(dst_buffer_size);
                if (NULL == dst_buffer) {
                    QZ_ERROR("Fail to allocate destination buffer with size "
                             "%u\n", dst_buffer_size);
                    ret = ERROR;
                    goto exit;
                }

                read_more = 0;
            } else {
                // corrupt data
                ret = ERROR;
                goto exit;
            }
        } else if (QZ_OK != ret) {
            QZ_ERROR("Process file error: %d\n", ret);
            ret = ERROR;
            goto exit;
        } else {
            read_more = 1;
        }

        file_remaining -= bytes_read;
        speed_size += bytes_read;

        if (g_read_o_direct && bytes_read % 4096 != 0) {
            fcntl(src_file->fd, F_SETFL, O_RDONLY);
            g_read_o_direct = 0;
        }
    } while (file_remaining > 0);

    if (dest_buffer->off != 0) {
        if (g_write_o_direct && dest_buffer->off % 4096 != 0) {
            fcntl(dst_file->fd, F_SETFL, O_WRONLY | O_CREAT | O_TRUNC);
            g_write_o_direct = 0;
        }
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring_);
        CHECK_GET_SQE(sqe)
        io_uring_prep_write(sqe, dst_file->fd, dest_buffer->buffer, dest_buffer->off, dst_file->off);
        int bytes_written = getIoUringResult(ring_);
        CHECK_IO_URING_WRITE_RETURN(bytes_written, dest_buffer->off, "compress")
        dst_file_size += bytes_written;
        dst_file->off += bytes_written;
        dest_buffer->off = 0;
    }

    displayStats(time_list_head, src_file_size, dst_file_size, is_compress);

exit:
    if (time_list_head) {
        freeTimeList(time_list_head);
    }
    if (src_file) {
        freeIoUringFile(src_file);
    }
    if (dst_file) {
        freeIoUringFile(dst_file);
    }
    if (src_buffer) {
        free(src_buffer);
    }
    if (dst_buffer) {
        free(dst_buffer);
    }
    if (!g_keep && OK == ret) {
        unlink(src_file_name);
    }
    if (ret) {
        exit(ret);
    }
}

int qatzipSetup(QzSession_T *sess, QzSessionParams_T *params)
{
    int status;

    QZ_DEBUG("mw>>> sess=%p\n", sess);
    status = qzInit(sess, getSwBackup(sess));
    if (QZ_OK != status && QZ_DUPLICATE != status) {
        QZ_ERROR("QAT init failed with error: %d\n", status);
        return ERROR;
    }
    QZ_DEBUG("QAT init OK with error: %d\n", status);

    status = qzSetupSession(sess, params);
    if (QZ_OK != status && QZ_DUPLICATE != status) {
        QZ_ERROR("Session setup failed with error: %d\n", status);
        return ERROR;
    }

    QZ_DEBUG("Session setup OK with error: %d\n", status);
    return 0;
}

int qatzipClose(QzSession_T *sess)
{
    qzTeardownSession(sess);
    qzClose(sess);

    return 0;
}

QzSuffix_T getSuffix(const char *filename)
{
    QzSuffix_T  s = E_SUFFIX_UNKNOWN;
    size_t len = strlen(filename);
    if (len >= strlen(SUFFIX_GZ) &&
        !strcmp(filename + (len - strlen(SUFFIX_GZ)), SUFFIX_GZ)) {
        s = E_SUFFIX_GZ;
    } else if (len >= strlen(SUFFIX_7Z) &&
               !strcmp(filename + (len - strlen(SUFFIX_7Z)), SUFFIX_7Z)) {
        s = E_SUFFIX_7Z;
    }
    return s;
}

bool hasSuffix(const char *fname)
{
    size_t len = strlen(fname);
    if (len >= strlen(SUFFIX_GZ) &&
        !strcmp(fname + (len - strlen(SUFFIX_GZ)), SUFFIX_GZ)) {
        return 1;
    } else if (len >= strlen(SUFFIX_7Z) &&
               !strcmp(fname + (len - strlen(SUFFIX_7Z)), SUFFIX_7Z)) {
        return 1;
    }
    return 0;
}

int makeOutName(const char *in_name, const char *out_name,
                char *oname, int is_compress)
{
    if (is_compress) {
        if (hasSuffix(in_name)) {
            QZ_ERROR("Warning: %s already has .gz suffix -- unchanged\n",
                     in_name);
            return -1;
        }
        /* add suffix */
        snprintf(oname, MAX_PATH_LEN, "%s%s", out_name ? out_name : in_name,
                 SUFFIX_GZ);
    } else {
        if (!hasSuffix(in_name)) {
            QZ_ERROR("Error: %s: Wrong suffix. Supported suffix: 7z/gz.\n",
                     in_name);
            return -1;
        }
        /* remove suffix */
        snprintf(oname, MAX_PATH_LEN, "%s", out_name ? out_name : in_name);
        if (NULL == out_name) {
            oname[strlen(in_name) - strlen(SUFFIX_GZ)] = '\0';
        }
    }

    return 0;
}

/* Makes a complete file system path by adding a file name to the path of its
 * parent directory. */
void mkPath(char *path, const char *dirpath, char *file)
{
    if (strlen(dirpath) + strlen(file) + 1 < MAX_PATH_LEN) {
        snprintf(path, MAX_PATH_LEN, "%s/%s", dirpath, file);
    } else {
        QZ_ERROR("The path exceeds the max len.\n");
        exit(QZ7Z_ERR_INVALID_SIZE);
    }
}


void processDir(QzSession_T *sess, const char *in_name,
                const char *out_name, int is_compress)
{
    DIR *dir;
    struct dirent *entry;
    char inpath[MAX_PATH_LEN];

    dir = opendir(in_name);
    if (!dir) {
        QZ_ERROR("Cannot open dir: %s.\n", in_name);
        exit(QZ7Z_ERR_OPEN);
    }

    while ((entry = readdir(dir))) {
        /* Ignore anything starting with ".", which includes the special
         * files ".", "..", as well as hidden files. */
        if (entry->d_name[0] == '.') {
            continue;
        }

        /* Qualify the file with its parent directory to obtain a complete
         * path. */
        mkPath(inpath, in_name, entry->d_name);

        processFile(sess, inpath, out_name, is_compress);
    }
}

void processFile(QzSession_T *sess, const char *in_name,
                 const char *out_name, int is_compress)
{
    int ret;
    struct stat fstat;

    ret = stat(in_name, &fstat);
    if (ret) {
        perror(in_name);
        exit(-1);
    }

    if (S_ISDIR(fstat.st_mode)) {
        processDir(sess, in_name, out_name, is_compress);
    } else {
        char oname[MAX_PATH_LEN];
        qzMemSet(oname, 0, MAX_PATH_LEN);

        if (makeOutName(in_name, out_name, oname, is_compress)) {
            return;
        }
        doProcessFile(sess, in_name, oname, is_compress);
    }
}

void processFileIoUring(QzSession_T *sess, const char *in_name,
                 const char *out_name, int is_compress, struct io_uring *ring_)
{
    int ret;
    struct stat fstat;

    ret = stat(in_name, &fstat);
    if (ret) {
        perror(in_name);
        exit(-1);
    }

    if (S_ISDIR(fstat.st_mode)) {
        processDir(sess, in_name, out_name, is_compress);
    } else {
        char oname[MAX_PATH_LEN];
        qzMemSet(oname, 0, MAX_PATH_LEN);

        if (makeOutName(in_name, out_name, oname, is_compress)) {
            return;
        }
        doProcessFileIoUring(sess, in_name, oname, is_compress, ring_);
    }
}

void version()
{
    char const *const *p = g_license_msg;

    QZ_PRINT("%s v%s\n", g_program_name, QATZIP_VERSION);
    while (*p) {
        QZ_PRINT("%s\n", *p++);
    }
}

char *qzipBaseName(char *fname)
{
    char *p;

    if ((p = strrchr(fname, '/')) != NULL) {
        fname = p + 1;
    }
    return fname;
}

void processStream(QzSession_T *sess, FILE *src_file, FILE *dst_file,
                   int is_compress)
{
    int ret = OK;
    unsigned int src_buffer_size = 0;
    unsigned int dst_buffer_size = 0;
    off_t dst_file_size = 0;
    unsigned char *src_buffer = NULL;
    unsigned char *dst_buffer = NULL;
    unsigned int bytes_read = 0, bytes_processed = 0;
    unsigned int ratio_idx = 0;
    const unsigned int ratio_limit =
        sizeof(g_bufsz_expansion_ratio) / sizeof(unsigned int);
    unsigned int read_more = 0;
    RunTimeList_T *time_list_head = malloc(sizeof(RunTimeList_T));
    if (NULL == time_list_head) {
        QZ_ERROR("Malloc time_list error.\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    gettimeofday(&time_list_head->time_s, NULL);
    time_list_head->time_e = time_list_head->time_s;
    time_list_head->next = NULL;
    int pending_in = 0;
    int bytes_input = 0;

    src_buffer_size = SRC_BUFF_LEN;
    if (is_compress) {
        dst_buffer_size = qzMaxCompressedLength(src_buffer_size, sess);
    } else { /* decompress */
        dst_buffer_size = src_buffer_size *
                          g_bufsz_expansion_ratio[ratio_idx++];
    }

    src_buffer = malloc(src_buffer_size);
    if (src_buffer == NULL) {
        QZ_ERROR("Malloc src_buffer error.\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }
    dst_buffer = malloc(dst_buffer_size);
    if (dst_buffer == NULL) {
        QZ_ERROR("Malloc dst_buffer error.\n");
        ret = QZ7Z_ERR_MALLOC;
        goto exit;
    }

    read_more = 1;
    while (!feof(stdin)) {
        if (read_more) {
            bytes_read = fread(src_buffer + pending_in, 1,
                               src_buffer_size - pending_in, src_file);
            if (0 == is_compress) {
                bytes_read += pending_in;
                bytes_input = bytes_read;
                pending_in = 0;
            }
        }

        ret = doProcessBuffer(sess, src_buffer, &bytes_read, dst_buffer,
                              dst_buffer_size, time_list_head, dst_file,
                              &dst_file_size, is_compress);

        if (QZ_DATA_ERROR == ret || QZ_BUF_ERROR == ret) {
            if (!is_compress) {
                pending_in = bytes_input - bytes_read;
            }
            bytes_processed += bytes_read;
            if (0 != bytes_read) {
                if (!is_compress && pending_in > 0) {
                    memmove(src_buffer, src_buffer + bytes_read,
                            src_buffer_size - bytes_read);
                }
                read_more = 1;
            } else if (QZ_BUF_ERROR == ret) {
                // dest buffer not long enough
                if (ratio_limit == ratio_idx) {
                    QZ_ERROR("Could not expand more destination buffer\n");
                    ret = ERROR;
                    goto exit;
                }

                free(dst_buffer);
                dst_buffer_size = src_buffer_size *
                                  g_bufsz_expansion_ratio[ratio_idx++];
                dst_buffer = malloc(dst_buffer_size);
                if (NULL == dst_buffer) {
                    QZ_ERROR("Fail to allocate destination buffer with size "
                             "%u\n", dst_buffer_size);
                    ret = ERROR;
                    goto exit;
                }

                read_more = 0;
            } else {
                // corrupt data
                ret = ERROR;
                goto exit;
            }
        } else if (QZ_OK != ret) {
            QZ_ERROR("Process file error: %d\n", ret);
            ret = ERROR;
            goto exit;
        } else {
            read_more = 1;
        }
    }

exit:
    if (time_list_head) {
        freeTimeList(time_list_head);
    }
    if (src_buffer) {
        free(src_buffer);
    }
    if (dst_buffer) {
        free(dst_buffer);
    }

    if (ret) {
        exit(ret);
    }
}

static IoUringFile_T *mallocIoUringFile(int fd)
{
    IoUringFile_T *io_uring_file = malloc(sizeof(IoUringFile_T));
    CHECK_ALLOC_RETURN_VALUE(io_uring_file)
    io_uring_file->fd = fd;
    io_uring_file->off = 0;
    return io_uring_file;
}

IoUringFile_T *generateIoUringFile(const char *file_name, int flags)
{
    int fd = open(file_name, flags);
    if(fd < 0) {
        return NULL;
    }

    return mallocIoUringFile(fd);
}

IoUringFile_T *generateIoUringFileWithMode(const char *file_name, int flags, mode_t mode)
{
    int fd = open(file_name, flags, mode);
    if(fd < 0) {
        return NULL;
    }

    return mallocIoUringFile(fd);
}

DestBuffer_T *generateDestBuffer(unsigned int size) {
    void* buffer = aligned_alloc(4096, size);
    if (buffer == NULL) {
        return NULL;
    }
    DestBuffer_T* dest_buffer = malloc(sizeof(DestBuffer_T));
    CHECK_ALLOC_RETURN_VALUE(dest_buffer);
    dest_buffer->buffer = buffer;
    dest_buffer->size = size;
    dest_buffer->off = 0;
    return dest_buffer;
}

void freeIoUringFile(IoUringFile_T *iouringf) 
{
    close(iouringf->fd);
    free(iouringf);
}

void freeDestBuffer(DestBuffer_T* dest_buffer) {
    free(dest_buffer->buffer);
    free(dest_buffer);
}

ssize_t getIoUringResult(struct io_uring *ring_)
{
    struct io_uring_cqe *cqe = NULL;
    ssize_t res = 0;
    if(io_uring_submit(ring_) != 1) {
        QZ_ERROR("Io_Uring submit failed\n");
        return -1;
    }
    io_uring_wait_cqe(ring_, &cqe);
    res = cqe -> res;
    io_uring_cqe_seen(ring_, cqe);
    return res;
}