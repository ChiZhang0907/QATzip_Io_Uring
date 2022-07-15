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


int main(int argc, char **argv)
{
    int ret = QZ_OK;
    int arg_count; /* number of files or directories to process */
    g_program_name = qzipBaseName(argv[0]);
    char *out_name = NULL;
    FILE *stream_out = NULL;
    int option_f = 0;
    Qz7zItemList_T *the_list;
    int is_good_7z = 0;
    int is_dir = 0;
    int recursive_mode = 0;
    errno = 0;

    if (qzGetDefaults(&g_params_th) != QZ_OK)
        return -1;

    while (true) {
        int optc;
        int long_idx = -1;
        char *stop = NULL;

        optc = getopt_long(argc, argv, g_short_opts, g_long_opts, &long_idx);
        if (optc < 0) {
            break;
        }
        switch (optc) {
        case 'd':
            g_decompress = 1;
            break;
        case 'h':
            help();
            exit(OK);
            break;
        case 'k':
            g_keep = 1;
            break;
        case 'i':
            g_io_uring = 0;
            break;
        case 'D':
            g_o_direct = 0;
            break;
        case 'V':
            version();
            exit(OK);
            break;
        case 'R':
            recursive_mode = 1;
            break;
        case 'A':
            if (strcmp(optarg, "deflate") == 0) {
                g_params_th.comp_algorithm = QZ_DEFLATE;
            } else {
                QZ_ERROR("Error service arg: %s\n", optarg);
                return -1;
            }
            break;
        case 'H':
            if (strcmp(optarg, "static") == 0) {
                g_params_th.huffman_hdr = QZ_STATIC_HDR;
            } else if (strcmp(optarg, "dynamic") == 0) {
                g_params_th.huffman_hdr = QZ_DYNAMIC_HDR;
            } else {
                QZ_ERROR("Error huffman arg: %s\n", optarg);
                return -1;
            }
            break;
        case 'O':
            if (strcmp(optarg, "gzip") == 0) {
                g_params_th.data_fmt = QZ_DEFLATE_GZIP;
            } else if (strcmp(optarg, "gzipext") == 0) {
                g_params_th.data_fmt = QZ_DEFLATE_GZIP_EXT;
            } else if (strcmp(optarg, "7z") == 0) {
                g_params_th.data_fmt = QZ_DEFLATE_RAW;
            } else {
                QZ_ERROR("Error gzip header format arg: %s\n", optarg);
                return -1;
            }
            break;
        case 'o':
            out_name = optarg;
            break;
        case 'L':
            g_params_th.comp_lvl = GET_LOWER_32BITS(strtoul(optarg, &stop, 0));
            if (*stop != '\0' || ERANGE == errno ||
                g_params_th.comp_lvl > QZ_DEFLATE_COMP_LVL_MAXIMUM ||
                g_params_th.comp_lvl <= 0) {
                QZ_ERROR("Error compLevel arg: %s\n", optarg);
                return -1;
            }
            break;
        case 'C':
            g_params_th.hw_buff_sz =
                GET_LOWER_32BITS(strtoul(optarg, &stop, 0));
            if (*stop != '\0' || ERANGE == errno ||
                g_params_th.hw_buff_sz > USDM_ALLOC_MAX_SZ / 2) {
                printf("Error chunk size arg: %s\n", optarg);
                return -1;
            }
            break;
        case 'S':
            g_speed = GET_LOWER_32BITS(strtoul(optarg, &stop, 0));
            if (*stop != '\0' || ERANGE == errno ||
                g_speed <= 0) {
                QZ_ERROR("Error speed limitation: %s\n", optarg);
                return -1;
            }
            break;
        case 'r':
            g_params_th.req_cnt_thrshold =
                GET_LOWER_32BITS(strtoul(optarg, &stop, 0));
            if (*stop != '\0' || errno ||
                g_params_th.req_cnt_thrshold > NUM_BUFF) {
                printf("Error request count threshold: %s\n", optarg);
                return -1;
            }
            break;
        case 'f':
            option_f = 1;
            break;
        case 'P':
            if (strcmp(optarg, "busy") == 0) {
                g_params_th.is_busy_polling = QZ_BUSY_POLLING;
            } else {
                printf("Error set polling mode: %s\n", optarg);
                return -1;
            }
            break;
        case 'a':
            if(qatzipSetup(&g_sess, &g_params_th)) {
                QZ_PRINT("The hardware is not availablt for QAT\n");
                exit(ERROR);
            } else {
                QZ_PRINT("The hardware is available for QAT\n");
                exit(0);
            }
        default:
            tryHelp();
        }
    }

    arg_count = argc - optind;
    if (0 == arg_count && isatty(fileno((FILE *)stdin))) {
        help();
        exit(OK);
    }

    if (g_decompress) {
        g_params_th.direction = QZ_DIR_DECOMPRESS;
    } else {
        g_params_th.direction = QZ_DIR_COMPRESS;
    }

    if (qatzipSetup(&g_sess, &g_params_th)) {
        exit(ERROR);
    }

    if(g_io_uring) {
        const int init_io_uring_res = io_uring_queue_init(256, &ring, 0);
        if(init_io_uring_res != 0) {
            g_io_uring = 0;
        }
    }
    
    if (0 == arg_count) {
        if (isatty(fileno((FILE *)stdout)) && 0 == option_f &&
            0 == g_decompress) {
            printf("qzip: compressed data not written to a terminal. "
                   "Use -f to force compression.\n");
            printf("For help, type: qzip -h\n");
        } else {
            stream_out = stdout;
            stdout = freopen(NULL, "w", stdout);
            processStream(&g_sess, stdin, stream_out, g_decompress == 0);
        }
    } else if (g_params_th.data_fmt == QZ_DEFLATE_RAW) { //compress into 7z
        QZ_DEBUG("going to compress files into 7z archive ...\n");

        if (!out_name) {
            QZ_ERROR("Should use '-o' to specify an output name\n");
            help();
            exit(ERROR);
        }

        if (qatzipSetup(&g_sess, &g_params_th)) {
            fprintf(stderr, "qatzipSetup session failed\n");
            exit(ERROR);
        }

        for (int i = 0, j = optind; i < arg_count; ++i, ++j) {
            if (access(argv[j], F_OK)) {
                QZ_ERROR("%s: No such file or directory\n", argv[j]);
                exit(ERROR);
            }
        }

        the_list = itemListCreate(arg_count, argv);
        if (!the_list) {
            exit(ERROR);
        }
        ret = qz7zCompress(&g_sess, the_list, out_name);
        itemListDestroy(the_list);
    } else {  // decompress from 7z; compress into gz; decompress from gz
        while (optind < argc) {

            if (access(argv[optind], F_OK)) {
                QZ_ERROR("%s: No such file or directory\n", argv[optind]);
                exit(ERROR);
            }

            QzSuffix_T  suffix = getSuffix(argv[optind]);

            is_dir = checkDirectory(argv[optind]);

            if (g_decompress && !is_dir && !hasSuffix(argv[optind])) {
                QZ_ERROR("Error: %s: Wrong suffix. Supported suffix: 7z/gz.\n",
                         argv[optind]);
                exit(ERROR);
            }

            if (g_decompress) {

                if (!recursive_mode)  {
                    if (suffix == E_SUFFIX_7Z) {

                        is_good_7z = check7zArchive(argv[optind]);
                        if (is_good_7z < 0) {
                            exit(ERROR);
                        }

                        if (arg_count > 1) {
                            fprintf(stderr, "only support decompressing ONE 7z "
                                    "archive for ONE command!\n");
                            exit(ERROR);
                        }

                        QZ_DEBUG(" this is a 7z archive, "
                                 "going to decompress ... \n");
                        g_params_th.data_fmt = QZ_DEFLATE_RAW;
                        if (qatzipSetup(&g_sess, &g_params_th)) {
                            fprintf(stderr, "qatzipSetup session  failed\n");
                            exit(ERROR);
                        }
                        ret = qz7zDecompress(&g_sess, argv[optind++]);
                    } else if (suffix == E_SUFFIX_GZ) {
                        processFile(&g_sess, argv[optind++], out_name,
                                    g_decompress == 0);
                    } else {
                        QZ_ERROR("Error: %s: Wrong suffix. Supported suffix: "
                                 "7z/gz.\n", argv[optind]);
                        exit(ERROR);
                    }
                }  else {
                    processFile(&g_sess, argv[optind++], out_name,
                                g_decompress == 0);
                }

            } else { // compress
                if(g_io_uring) {
                    processFileIoUring(&g_sess, argv[optind++], out_name,
                            g_decompress == 0, &ring);
                } else {
                    processFile(&g_sess, argv[optind++], out_name,
                            g_decompress == 0);
                }
            }
        }
    }

    if(g_io_uring) {
        io_uring_queue_exit(&ring);
    }

    if (qatzipClose(&g_sess)) {
        exit(ERROR);
    }

    return ret;
}
