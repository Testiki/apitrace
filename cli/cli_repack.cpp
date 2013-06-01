/**************************************************************************
 *
 * Copyright 2011 Jose Fonseca
 * All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 **************************************************************************/


#include <string.h>
#include <getopt.h>

#include <iostream>

#include "cli.hpp"

#include "trace_file.hpp"
#include "trace_threaded_file.hpp"

static trace::File::Compressor compressor = trace::File::LZ4HC;

static const char *synopsis = "Repack a trace file with other compression.";

static void
usage(void)
{
    std::cout
        << "usage: apitrace repack [OPTION] <in-trace-file> <out-trace-file>\n"
        << synopsis << "\n"
        << "\n"
        << "Snappy compression allows for faster replay and smaller memory footprint,\n"
        << "at the expense of a slightly smaller compression ratio than zlib.\n"
        << "LZ4HC compression allows for smaller file size than Snappy,\n"
        << "but since LZ4HC have small compression speed, it isn't of use to tracing.\n"
        << "\n"
        << "    -c, --compression=COMPRESSION  specify compression format.\n"
        << "                                   May be LZ4, Snappy or LZ4HC\n"
        << "                                   (LZ4HC by default)\n"
        << "\n";
}

const static char *
shortOptions = "hc:";

const static struct option
longOptions[] = {
    {"help", no_argument, 0, 'h'},
    {"compression", required_argument, 0, 'c'},
    {0, 0, 0, 0}
};

static int
repack(const char *inFileName, const char *outFileName)
{
    trace::File *inFile = trace::File::createForRead(inFileName);
    if (!inFile) {
        return 1;
    }

    trace::File *outFile;
    switch (compressor) {
    case trace::File::LZ4:
        outFile = new trace::ThreadedFile(new trace::LZ4Library(false));
        break;
    case trace::File::LZ4HC:
        outFile = new trace::ThreadedFile(new trace::LZ4Library(true));
        break;
    case trace::File::SNAPPY:
        outFile = new trace::ThreadedFile(new trace::SnappyLibrary());
        break;
    default:
        assert(false);
    }

    if (!outFile) {
        delete inFile;
        return 1;
    }
    outFile->open(outFileName, trace::File::Write);
    size_t size = 8192;
    char *buf = new char[size];
    size_t read;

    while ((read = inFile->read(buf, size)) != 0) {
        outFile->write(buf, read);
    }
    outFile->close();
    delete [] buf;
    delete outFile;
    delete inFile;

    return 0;
}

static int
command(int argc, char *argv[])
{
    int opt;
    while ((opt = getopt_long(argc, argv, shortOptions, longOptions, NULL)) != -1) {
        switch (opt) {
        case 'h':
            usage();
            return 0;
        case 'c':
            if (!strcmp(optarg, "LZ4HC")) {
              compressor = trace::File::LZ4HC;
            }
            else if (!strcmp(optarg, "LZ4")) {
              compressor = trace::File::LZ4;
            }
            else if (!strcmp(optarg, "Snappy")) {
              compressor = trace::File::SNAPPY;
            }
            else {
                std::cerr << "error: unrecognized compression library " << optarg << "`\n";
                usage();
                return 1;
            }
            break;
        default:
            std::cerr << "error: unexpected option `" << opt << "`\n";
            usage();
            return 1;
        }
    }

    if (argc != optind + 2) {
        std::cerr << "error: insufficient number of arguments\n";
        usage();
        return 1;
    }

    return repack(argv[optind], argv[optind + 1]);
}

const Command repack_command = {
    "repack",
    synopsis,
    usage,
    command
};
