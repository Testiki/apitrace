/**************************************************************************
 *
 * Copyright 2012 Samsung 
 * Contributed by Vladimir Platonov
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

/*
 * Threaded file represents trace file as number of data chunks (similar to
 * original Snappy file), but separate thread (and different compression
 * libraries) are used for compression.
 * Compression cache consists of two CACHE_SIZE size buffers, so when one
 * of them is full, tracing thread uses another while first one is
 * compressed by compression thread. Each buffer has read access and write
 * access semaphores, which are set by corresponding thread.
 *
 * For now Threaded file uses only for tracing purposes and any attempt to
 * call read functions will cause an exception.
 *
 * rawWrite function implemented to get rid of virtual write calls and
 * gain performance
 *
 * Compressor type set via APITRACE_COMPRESSOR environment variable.
 */

#ifndef TRACE_THREADED_FILE_HPP_
#define TRACE_THREADED_FILE_HPP_

#include <assert.h>
#include <string.h>
#include <cstdlib>
#include "os.hpp"
#include "os_thread.hpp"
#include "trace_file.hpp"

using namespace trace;

class CompressionCache {

private:
    static const int NUM_BUFFERS = 2;
    size_t m_chunkSize;

    char *m_caches[NUM_BUFFERS];
    char *m_cachePtr[NUM_BUFFERS];
    os::Mutex m_writeAccess[NUM_BUFFERS];
    os::Mutex m_readAccess[NUM_BUFFERS];

    size_t m_sizes[NUM_BUFFERS];

    int m_writeID;
    int m_readID;

    trace::CompressionLibrary * m_library;

    inline size_t usedCacheSize(int id) const {
        assert(m_cachePtr[id] >= m_caches[id]);
        return m_cachePtr[id] - m_caches[id];
    }

    inline size_t freeCacheSize(int id) const {
        assert(m_chunkSize >= usedCacheSize(id));
        return m_chunkSize - usedCacheSize(id);
    }

    inline void nextWriteBuffer() {
        m_sizes[m_writeID] = m_cachePtr[m_writeID] - m_caches[m_writeID];
        m_cachePtr[m_writeID] = m_caches[m_writeID];
        MutexUnlock(m_readAccess[m_writeID]);
        m_writeID = (m_writeID + 1) % NUM_BUFFERS;
        MutexLock(m_writeAccess[m_writeID]);
        m_sizes[m_writeID] = 0;
    }

    inline void nextReadBuffer() {
        m_cachePtr[m_readID] = m_caches[m_readID];
        MutexUnlock(m_writeAccess[m_readID]);
        m_readID = (m_readID + 1) % NUM_BUFFERS;
        MutexLock(m_readAccess[m_readID]);
    }

public:

    CompressionCache(size_t chunkSize, trace::CompressionLibrary *library);
    ~CompressionCache();
    void write(const char *buffer, size_t length);
    size_t read(char *buffer, size_t length);
    size_t readAndCompressBuffer(char *buffer, size_t &inputLength);

    void flushWrite() {
        nextWriteBuffer();
    }

    void acquireWriteControl() {
        MutexLock(m_writeAccess[m_writeID]);
        MutexLock(m_readAccess[m_writeID]);
        MutexLock(m_readAccess[m_writeID+1]);
    }

    void releaseLocks() {
        MutexUnlock(m_readAccess[m_writeID]);
    }

    void acquireReadControl() {
        MutexLock(m_readAccess[m_readID]);
    }
};

class ThreadedFile : public File {
public:
    ThreadedFile(trace::CompressionLibrary * lib = new LZ4Library(false),
                    const std::string &filename = std::string(),
                    File::Mode mode = File::Read);
    virtual ~ThreadedFile();

    virtual bool supportsOffsets() const;
    virtual File::Offset currentOffset();
    virtual void setCurrentOffset(const File::Offset &offset);

    bool write(const void *buffer, size_t length)
    {
        if (!m_isOpened || m_mode != File::Write) {
            return false;
        }
        return ThreadedFile::rawWrite(buffer, length);
    }

protected:
    virtual bool rawOpen(const std::string &filename, File::Mode mode);
    virtual bool rawWrite(const void *buffer, size_t length)
    {
        m_cache->write((const char *)buffer, length);
        return true;
    }

    virtual size_t rawRead(void *buffer, size_t length);
    virtual int rawGetc();
    virtual void rawClose();
    virtual void rawFlush();
    virtual bool rawSkip(size_t length);
    virtual int rawPercentRead();

private:
    std::fstream m_stream;
    std::streampos m_endPos;
    CompressionCache *m_cache;
    File::Offset m_currentOffset;
    static const size_t CACHE_SIZE = 1 * 1024 * 1024;
    trace::CompressionLibrary *m_library;
    os::Thread m_thread;

    void writeLength(size_t length);

    THREAD_ROUTINE static void * compressorThread(void * param);
 };


#endif /* TRACE_THREADED_FILE_HPP_ */
