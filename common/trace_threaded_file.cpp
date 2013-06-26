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

#include "trace_threaded_file.hpp"

using namespace trace;



CompressionCache::CompressionCache(size_t chunkSize, trace::CompressionLibrary *library) {
    m_chunkSize = chunkSize;
    m_library = library;
    for (int i = 0; i < NUM_BUFFERS; ++i) {
        m_caches[i] = new char[m_chunkSize];
        m_cachePtr[i] = m_caches[i];
        MutexInit(m_readAccess[i]);
        MutexInit(m_writeAccess[i]);
    }
    m_writeID = 0;
    m_readID = 0;
}

CompressionCache::~CompressionCache() {
    for (int i = 0; i < NUM_BUFFERS; ++i) {
        delete [] m_caches[i];
    }
}

void CompressionCache::write(const char *buffer, size_t length) {
    if (freeCacheSize(m_writeID) > length) {
        memcpy(m_cachePtr[m_writeID], buffer, length);
        m_cachePtr[m_writeID] += length;
    }
    else if (freeCacheSize(m_writeID) == length) {
        memcpy(m_cachePtr[m_writeID], buffer, length);
        m_cachePtr[m_writeID] += length;
        nextWriteBuffer();
    }
    else {
        size_t sizeToWrite = length;
        while (sizeToWrite >= freeCacheSize(m_writeID)) {
            size_t endSize = freeCacheSize(m_writeID);
            size_t offset = length - sizeToWrite;
            memcpy(m_cachePtr[m_writeID], (const char*)buffer + offset, endSize);
            sizeToWrite -= endSize;
            m_cachePtr[m_writeID] += endSize;
            nextWriteBuffer();
        }
        if (sizeToWrite) {
            size_t offset = length - sizeToWrite;
            memcpy(m_cachePtr[m_writeID], (const char*)buffer + offset, sizeToWrite);
            m_cachePtr[m_writeID] += sizeToWrite;
        }
    }
}

size_t CompressionCache::read(char *buffer, size_t length) {
    if (freeCacheSize(m_readID) >= length) {
        memcpy(buffer, m_cachePtr[m_readID], length);
        m_cachePtr[m_readID] += length;
    }
    else {
        size_t sizeToRead = length;
        size_t offset = 0;
        while (sizeToRead) {
            size_t chunkSize = std::min(freeCacheSize(m_readID), sizeToRead);
            offset = length - sizeToRead;
            memcpy((char*)buffer + offset, m_cachePtr[m_readID], chunkSize);
            m_cachePtr[m_readID] += chunkSize;
            sizeToRead -= chunkSize;
            if (sizeToRead > 0) {
                nextReadBuffer();
            }
        }
    }
    return length;
}

size_t CompressionCache::readAndCompressBuffer(char *buffer, size_t &inputLength) {
    size_t compressedLength;
    if (m_sizes[m_readID] == 0) {
        return 0;
    }
    else {
        m_library->compress(m_caches[m_readID], m_sizes[m_readID], buffer, &compressedLength);
    }
    inputLength = m_sizes[m_readID];
    nextReadBuffer();
    return compressedLength;
}

THREAD_ROUTINE void * ThreadedFile::compressorThread(void * param) {
    ThreadedFile *file = (ThreadedFile *) param;
    char * compressedData = new char[file->m_library->maxCompressedLength(file->CACHE_SIZE)];
    file->m_cache->acquireReadControl();
    size_t compressedLength = 0;
    size_t inputLength = 0;
    do {
        compressedLength = file->m_cache->readAndCompressBuffer(compressedData, inputLength);
        file->writeLength(compressedLength);
        file->m_stream.write(compressedData, compressedLength);
    }
    // The fact that we compress partially filled buffer means the end of tracing
    // Inputlenght may be 0 and it doesn't break the trace
    while (inputLength == CACHE_SIZE);

    delete [] compressedData;
    return NULL;
}

ThreadedFile::ThreadedFile(trace::CompressionLibrary * lib, const std::string &filename,
        File::Mode mode)
    : File() {
    m_library = lib;
    m_cache = NULL;
}

bool ThreadedFile::rawOpen(const std::string &filename, enum Mode mode) {
    std::ios_base::openmode fmode = std::fstream::binary;
    if (mode == File::Write) {
        fmode |= (std::fstream::out | std::fstream::trunc);
    }
    else if (mode == File::Read) {
        return false;
    }

    m_stream.open(filename.c_str(), fmode);

    if (m_stream.is_open() && mode == File::Read) {
        return false;
    } else if (m_stream.is_open() && mode == File::Write) {
        m_cache = new CompressionCache(CACHE_SIZE, m_library);
        unsigned int sig = m_library->getSignature();
        m_stream << ((unsigned char)((sig >> 8) & 0xFF));
        m_stream << ((unsigned char)(sig & 0xFF));
        m_cache->acquireWriteControl();
        m_thread = os::ThreadCreate(compressorThread, this);
    }
    return m_stream.is_open();
}


size_t ThreadedFile::rawRead(void *buffer, size_t length) {
    os::log("apitrace: threaded file read function access \n");
    os::abort();
}

int ThreadedFile::rawGetc() {
    os::log("apitrace: threaded file read function access \n");
    os::abort();
}

void ThreadedFile::rawClose() {
    if (!m_stream.is_open()) {
        return;
    }
    rawFlush();
    m_cache->releaseLocks();
    m_stream.close();
    delete m_cache;
}

ThreadedFile::~ThreadedFile() {

}

void ThreadedFile::rawFlush() {
    assert(m_mode == File::Write);
    m_cache->flushWrite();
    os::ThreadWait(m_thread);
    m_stream.flush();
}

bool ThreadedFile::rawSkip(size_t length) {
    os::log("apitrace: threaded file read function access \n");
    os::abort();
}

bool ThreadedFile::supportsOffsets() const {
    return false;
}

File::Offset ThreadedFile::currentOffset() {
    os::log("apitrace: threaded file read function access \n");
    os::abort();
}

void ThreadedFile::setCurrentOffset(const File::Offset &offset) {
    os::log("apitrace: threaded file read function access \n");
    os::abort();
}

void ThreadedFile::writeLength(size_t length) {
    unsigned char buf[m_library->m_lengthSize];
    m_library->setLength(buf, length);
    m_stream.write((const char *)buf, sizeof buf);
}

int ThreadedFile::rawPercentRead() {
    os::log("apitrace: threaded file read function access \n");
    os::abort();
}

ThreadedFile* File::createThreadedFile() {
    char *compressor = std::getenv("APITRACE_COMPRESSOR");
    if (compressor == NULL) {
        return new ThreadedFile(new SnappyLibrary());
    }
    if (strcmp(compressor, "LZ4HC") == 0) {
        return new ThreadedFile(new LZ4Library(true));
    }
    else if (strcmp(compressor, "LZ4") == 0) {
        return new ThreadedFile(new LZ4Library(false));
    }
    else {
        return new ThreadedFile(new SnappyLibrary());
    }
}
