/**************************************************************************
 *
 * Copyright 2011 Zack Rusin
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
 * Unified file for tracing/retracing with multiple compression
 * library support.
 *
 * Currently, LZ4 and Snappy are supported.
 * Zlib is supported only for read by specified class in trace_file_zlib.cpp
 *
 * The file is composed of a number of chunks, they are:
 * chunk {
 *     uint32 - specifying the length of the compressed data
 *     compressed data, in little endian
 * }
 *
 * File can contain any number of such chunks.
 *
 * Note:
 * Currently the default size for a a to-be-compressed data is
 * 1mb, meaning that the compressed data will be <= 1mb.
 * The reason it's 1mb is because it seems
 * to offer a pretty good compression/disk io speed ratio
 * but that might change.

 */

#include <iostream>

#include <assert.h>
#include <string.h>

#include "trace_file.hpp"
#include "trace_compression_library.hpp"

using namespace trace;


class CommonFile : public File {
public:
    CommonFile(CompressionLibrary * lib = new LZ4Library(false),
                const std::string &filename = std::string(),
                File::Mode mode = File::Read);
    virtual ~CommonFile();

    virtual bool supportsOffsets() const;
    virtual File::Offset currentOffset();
    virtual void setCurrentOffset(const File::Offset &offset);
protected:
    virtual bool rawOpen(const std::string &filename, File::Mode mode);
    virtual bool rawWrite(const void *buffer, size_t length);
    virtual size_t rawRead(void *buffer, size_t length);
    virtual int rawGetc();
    virtual void rawClose();
    virtual void rawFlush();
    virtual bool rawSkip(size_t length);
    virtual int rawPercentRead();

private:
    inline size_t usedCacheSize() const
    {
        assert(m_cachePtr >= m_cache);
        return m_cachePtr - m_cache;
    }
    inline size_t freeCacheSize() const
    {
        assert(m_cacheSize >= usedCacheSize());
        if (m_cacheSize > 0) {
            return m_cacheSize - usedCacheSize();
        } else {
            return 0;
        }
    }
    inline bool endOfData() const
    {
        return m_stream.eof() && freeCacheSize() == 0;
    }
    void flushWriteCache();
    void flushReadCache(size_t skipLength = 0);
    void createCache(size_t size);
    void reallocCompressedCache(size_t size);
    void writeLength(size_t length);
    size_t readLength();
private:
    std::fstream m_stream;
    size_t m_cacheMaxSize;
    size_t m_cacheSize;
    char *m_cache;
    char *m_cachePtr;

    size_t m_compressedCacheSize;
    char *m_compressedCache;

    File::Offset m_currentOffset;
    std::streampos m_endPos;

    CompressionLibrary * m_library;
    static const size_t CACHE_SIZE = 1 * 1024 * 1024;

};

CommonFile::CommonFile(CompressionLibrary * lib, const std::string &filename,
                              File::Mode mode)
    : File(),
      m_cacheMaxSize(CACHE_SIZE),
      m_cacheSize(m_cacheMaxSize),
      m_cache(new char [m_cacheMaxSize]),
      m_cachePtr(m_cache),
      m_compressedCacheSize(CACHE_SIZE),
      m_compressedCache(new char[m_compressedCacheSize]),
      m_library(lib)
{
}

CommonFile::~CommonFile()
{
    close();
    delete [] m_compressedCache;
    delete [] m_cache;
    delete m_library;
}

bool CommonFile::rawOpen(const std::string &filename, File::Mode mode)
{
    std::ios_base::openmode fmode = std::fstream::binary;
    if (mode == File::Write) {
        fmode |= (std::fstream::out | std::fstream::trunc);
        createCache(CACHE_SIZE);
    } else if (mode == File::Read) {
        fmode |= std::fstream::in;
    }

    m_stream.open(filename.c_str(), fmode);

    //read in the initial buffer if we're reading
    if (m_stream.is_open() && mode == File::Read) {
        m_stream.seekg(0, std::ios::end);
        m_endPos = m_stream.tellg();
        m_stream.seekg(0, std::ios::beg);

        unsigned int sig = m_library->getSignature();
        unsigned char byte1, byte2;
        m_stream >> byte1;
        m_stream >> byte2;
        assert(byte1 == (unsigned char)((sig >> 8) & 0xFF));
        assert(byte2 == (unsigned char)(sig & 0xFF));

        flushReadCache();
    } else if (m_stream.is_open() && mode == File::Write) {
        unsigned int sig = m_library->getSignature();
        m_stream << ((unsigned char)((sig >> 8) & 0xFF));
        m_stream << ((unsigned char)(sig & 0xFF));
    }
    return m_stream.is_open();
}

bool CommonFile::rawWrite(const void *buffer, size_t length)
{
    if (freeCacheSize() > length) {
        memcpy(m_cachePtr, buffer, length);
        m_cachePtr += length;
    } else if (freeCacheSize() == length) {
        memcpy(m_cachePtr, buffer, length);
        m_cachePtr += length;
        flushWriteCache();
    } else {
        size_t sizeToWrite = length;

        while (sizeToWrite >= freeCacheSize()) {
            size_t endSize = freeCacheSize();
            size_t offset = length - sizeToWrite;
            memcpy(m_cachePtr, (const char*)buffer + offset, endSize);
            sizeToWrite -= endSize;
            m_cachePtr += endSize;
            flushWriteCache();
        }
        if (sizeToWrite) {
            size_t offset = length - sizeToWrite;
            memcpy(m_cachePtr, (const char*)buffer + offset, sizeToWrite);
            m_cachePtr += sizeToWrite;
        }
    }

    return true;
}

size_t CommonFile::rawRead(void *buffer, size_t length)
{
    if (endOfData()) {
        return 0;
    }

    if (freeCacheSize() >= length) {
        memcpy(buffer, m_cachePtr, length);
        m_cachePtr += length;
    } else {
        size_t sizeToRead = length;
        size_t offset = 0;
        while (sizeToRead) {
            size_t chunkSize = std::min(freeCacheSize(), sizeToRead);
            offset = length - sizeToRead;
            memcpy((char*)buffer + offset, m_cachePtr, chunkSize);
            m_cachePtr += chunkSize;
            sizeToRead -= chunkSize;
            if (sizeToRead > 0) {
                flushReadCache();
            }
            if (!m_cacheSize) {
                return length - sizeToRead;
            }
        }
    }

    return length;
}

int CommonFile::rawGetc()
{
    unsigned char c = 0;
    if (rawRead(&c, 1) != 1)
        return -1;
    return c;
}

void CommonFile::rawClose()
{
    if (!m_stream.is_open()) {
        return;
    }
    if (m_mode == File::Write) {
        flushWriteCache();
    }
    m_stream.close();
    delete [] m_cache;
    m_cache = NULL;
    m_cachePtr = NULL;
}

void CommonFile::rawFlush()
{
    assert(m_mode == File::Write);
    flushWriteCache();
    m_stream.flush();
}

void CommonFile::flushWriteCache()
{
    size_t inputLength = usedCacheSize();

    if (inputLength) {
        size_t compressedLength;

        m_library->compress(m_cache, inputLength, m_compressedCache, &compressedLength);
        writeLength(compressedLength);
        m_stream.write(m_compressedCache, compressedLength);
        m_cachePtr = m_cache;
    }
    assert(m_cachePtr == m_cache);
}

void CommonFile::flushReadCache(size_t skipLength)
{
    m_currentOffset.chunk = m_stream.tellg();
    size_t compressedLength;
    compressedLength = readLength();

    if (compressedLength) {
        reallocCompressedCache(compressedLength);
        m_stream.read((char*)m_compressedCache, compressedLength);
        int cacheSize = m_library->uncompressedLength(m_compressedCache, compressedLength);
        createCache(cacheSize);
        if (skipLength < cacheSize) {
            m_library->uncompress(m_compressedCache, compressedLength, m_cache);
        }
    } else {
        createCache(0);
    }
}

void CommonFile::createCache(size_t size)
{
    if (size > m_cacheMaxSize) {
        do {
            m_cacheMaxSize <<= 1;
        } while (size > m_cacheMaxSize);

        delete [] m_cache;
        m_cache = new char[m_cacheMaxSize];
    }

    m_cachePtr = m_cache;
    m_cacheSize = size;
}

void CommonFile::reallocCompressedCache(size_t size)
{
    if (size > m_compressedCacheSize) {
        do {
            m_compressedCacheSize <<= 1;
        } while (size > m_compressedCacheSize);

        delete [] m_compressedCache;
        m_compressedCache = new char[m_compressedCacheSize];
    }
}

void CommonFile::writeLength(size_t length)
{
    unsigned char buf[m_library->m_lengthSize];
    m_library->setLength(buf, length);
    m_stream.write((const char *)buf, sizeof buf);
}

size_t CommonFile::readLength()
{
    unsigned char buf[m_library->m_lengthSize];
    size_t length;
    m_stream.read((char *)buf, sizeof buf);
    if (m_stream.fail()) {
        length = 0;
    } else {
        length = m_library->getLength(buf);
    }
    return length;
}

bool CommonFile::supportsOffsets() const
{
    return true;
}

File::Offset CommonFile::currentOffset()
{
    m_currentOffset.offsetInChunk = m_cachePtr - m_cache;
    return m_currentOffset;
}

void CommonFile::setCurrentOffset(const File::Offset &offset)
{
    // to remove eof bit
    m_stream.clear();
    // seek to the start of a chunk
    m_stream.seekg(offset.chunk, std::ios::beg);
    // load the chunk
    flushReadCache();
    assert(m_cacheSize >= offset.offsetInChunk);
    // seek within our cache to the correct location within the chunk
    m_cachePtr = m_cache + offset.offsetInChunk;

}

bool CommonFile::rawSkip(size_t length)
{
    if (endOfData()) {
        return false;
    }

    if (freeCacheSize() >= length) {
        m_cachePtr += length;
    } else {
        size_t sizeToRead = length;
        while (sizeToRead) {
            size_t chunkSize = std::min(freeCacheSize(), sizeToRead);
            m_cachePtr += chunkSize;
            sizeToRead -= chunkSize;
            if (sizeToRead > 0) {
                flushReadCache(sizeToRead);
            }
            if (!m_cacheSize) {
                break;
            }
        }
    }

    return true;
}

int CommonFile::rawPercentRead()
{
    return 100 * (double(m_stream.tellg()) / double(m_endPos));
}


File* File::createCommonFile(File::Compressor compressor)
{
    switch (compressor) {
        case SNAPPY:
            return new CommonFile(new SnappyLibrary());
        case LZ4:
            return new CommonFile(new LZ4Library(false));
        case LZ4HC:
            return new CommonFile(new LZ4Library(true));
        default:
            return new CommonFile(new SnappyLibrary());
    }
}
