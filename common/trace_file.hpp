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


#ifndef TRACE_FILE_HPP
#define TRACE_FILE_HPP

#include <string>
#include <fstream>
#include <stdint.h>

#include <snappy.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zlib.h>
#include <assert.h>


namespace trace {

class ThreadedFile;

class CompressionLibrary {

public:
    virtual void compress(const char *src, size_t inputLength, char *dst, size_t *outLength) = 0;
    virtual void uncompress(const char *src, size_t inputLength, char *dst) = 0;
    virtual size_t maxCompressedLength(size_t inputLength) = 0;
    virtual size_t uncompressedLength(const char *src, size_t inputLength) = 0;
    virtual ~CompressionLibrary(){};

    static const size_t m_lengthSize = 4;

    size_t getLength(const unsigned char* src) const {
        size_t length;
        length  =  (size_t)src[0];
        length |= ((size_t)src[1] <<  8);
        length |= ((size_t)src[2] << 16);
        length |= ((size_t)src[3] << 24);
        return length;
    }

    void setLength(unsigned char* dst, size_t length) {
        dst[0] = length & 0xff; length >>= 8;
        dst[1] = length & 0xff; length >>= 8;
        dst[2] = length & 0xff; length >>= 8;
        dst[3] = length & 0xff; length >>= 8;
    }

    static const unsigned char m_sigByte1 = 'z';
    static const unsigned char m_sigByte2 = 'z';
    virtual unsigned int getSignature() = 0;

    const char *name;
};

class SnappyLibrary : public CompressionLibrary {

public:

    static const unsigned char m_sigByte1 = 'a';
    static const unsigned char m_sigByte2 = 't';

    SnappyLibrary()
    {
        name = "SNAPPY";
    }

    virtual void compress(const char *src, size_t inputLength, char *dst, size_t *outLength)
    {
        ::snappy::RawCompress(src, inputLength, dst, outLength);
    }

    virtual void uncompress(const char *src, size_t inputLength, char *dst)
    {
        ::snappy::RawUncompress(src, inputLength, dst);
    }

    virtual size_t maxCompressedLength(size_t inputLength)
    {
        return ::snappy::MaxCompressedLength(inputLength);
    }

    virtual size_t uncompressedLength(const char *src, size_t inputLength)
    {
        size_t result;
        ::snappy::GetUncompressedLength(src, inputLength, &result);
        return result;
    }

    static bool isSnappyCompressed(char b1, char b2)
    {
        return (b1 == m_sigByte1 && b2 == m_sigByte2);
    }

    virtual unsigned int getSignature()
    {
        return (m_sigByte1 << 8) + m_sigByte2;
    }
};

class LZ4Library : public CompressionLibrary
{

private:
    static const size_t m_chunkSize = 1 * 1024 * 1024;
    bool m_highCompression;

public:

    static const unsigned char m_sigByte1 = 'l';
    static const unsigned char m_sigByte2 = 'z';

    LZ4Library(bool highCompression) : m_highCompression(highCompression)
    {
        name = "LZ4";
    }

    virtual void compress(const char *src, size_t inputLength, char *dst, size_t *outLength)
    {
        setLength((unsigned char*)dst, inputLength);
        if (m_highCompression) {
            *outLength = LZ4_compressHC(src, dst + m_lengthSize, inputLength);
        }
        else {
            *outLength = LZ4_compress(src, dst + m_lengthSize, inputLength);
        }
        *outLength += m_lengthSize;
    }

    virtual void uncompress(const char *src, size_t inputLength, char *dst)
    {
        LZ4_uncompress_unknownOutputSize(src + m_lengthSize, dst, inputLength - m_lengthSize, m_chunkSize);
    }

    virtual size_t maxCompressedLength(size_t inputLength)
    {
        return LZ4_compressBound(inputLength) + m_lengthSize;
    }

    virtual size_t uncompressedLength(const char *src, size_t inputLength)
    {
        return getLength((const unsigned char*)src);
    }

    static bool isLZ4Compressed(char b1, char b2)
    {
        return (b1 == m_sigByte1 && b2 == m_sigByte2);
    }

    virtual unsigned int getSignature()
    {
        return (m_sigByte1 << 8) + m_sigByte2;
    }

};

class ZLibrary : public CompressionLibrary
{

private:
    unsigned long i;
public:

    static const unsigned char m_sigByte1 = 0x1f;
    static const unsigned char m_sigByte2 = 0x8b;

    ZLibrary()
    {
        name = "Zlib";
    }

    virtual void compress(const char *src, size_t inputLength, char *dst, size_t *outLength)
    {
        assert(false);
    }

    virtual void uncompress(const char *src, size_t inputLength, char *dst)
    {
        assert(false);
    }

    virtual size_t maxCompressedLength(size_t inputLength)
    {
        assert(false);
        return 0;
    }

    virtual size_t uncompressedLength(const char *src, size_t inputLength)
    {
        assert(false);
        return 0;
    }

    static bool isZlibCompressed(unsigned char b1, unsigned char b2)
    {
        return (b1 == m_sigByte1 && b2 == m_sigByte2);
    }

    virtual unsigned int getSignature()
    {
        return (m_sigByte1 << 8) + m_sigByte2;
    }

};

class File {
public:
    enum Mode {
        Read,
        Write
    };
    struct Offset {
        Offset(uint64_t _chunk = 0, uint32_t _offsetInChunk = 0)
            : chunk(_chunk),
              offsetInChunk(_offsetInChunk)
        {}
        uint64_t chunk;
        uint32_t offsetInChunk;
    };
    enum Compressor {
        SNAPPY,
        LZ4,
        LZ4HC,
        ZLIB
    };

public:
    static File *createZLib(void);
    static File *createForRead(const char *filename);
    static ThreadedFile *createThreadedFile();
    static File *createCommonFile(File::Compressor compressor);
public:
    File(const std::string &filename = std::string(),
         File::Mode mode = File::Read);
    virtual ~File();

    bool isOpened() const;
    File::Mode mode() const;

    bool open(const std::string &filename, File::Mode mode);
    bool write(const void *buffer, size_t length);
    size_t read(void *buffer, size_t length);
    void close();
    void flush(void);
    int getc();
    bool skip(size_t length);
    int percentRead();

    virtual bool supportsOffsets() const = 0;
    virtual File::Offset currentOffset() = 0;
    virtual void setCurrentOffset(const File::Offset &offset);
protected:
    virtual bool rawOpen(const std::string &filename, File::Mode mode) = 0;
    virtual bool rawWrite(const void *buffer, size_t length) = 0;
    virtual size_t rawRead(void *buffer, size_t length) = 0;
    virtual int rawGetc() = 0;
    virtual void rawClose() = 0;
    virtual void rawFlush() = 0;
    virtual bool rawSkip(size_t length) = 0;
    virtual int rawPercentRead() = 0;

protected:
    File::Mode m_mode;
    bool m_isOpened;
};

inline bool File::isOpened() const
{
    return m_isOpened;
}

inline File::Mode File::mode() const
{
    return m_mode;
}

inline bool File::open(const std::string &filename, File::Mode mode)
{
    if (m_isOpened) {
        close();
    }
    m_isOpened = rawOpen(filename, mode);
    m_mode = mode;

    return m_isOpened;
}

inline bool File::write(const void *buffer, size_t length)
{
    if (!m_isOpened || m_mode != File::Write) {
        return false;
    }
    return rawWrite(buffer, length);
}

inline size_t File::read(void *buffer, size_t length)
{
    if (!m_isOpened || m_mode != File::Read) {
        return 0;
    }
    return rawRead(buffer, length);
}

inline int File::percentRead()
{
    if (!m_isOpened || m_mode != File::Read) {
        return 0;
    }
    return rawPercentRead();
}

inline void File::close()
{
    if (m_isOpened) {
        rawClose();
        m_isOpened = false;
    }
}

inline void File::flush(void)
{
    if (m_mode == File::Write) {
        rawFlush();
    }
}

inline int File::getc()
{
    if (!m_isOpened || m_mode != File::Read) {
        return -1;
    }
    return rawGetc();
}

inline bool File::skip(size_t length)
{
    if (!m_isOpened || m_mode != File::Read) {
        return false;
    }
    return rawSkip(length);
}


inline bool
operator<(const File::Offset &one, const File::Offset &two)
{
    return one.chunk < two.chunk ||
            (one.chunk == two.chunk && one.offsetInChunk < two.offsetInChunk);
}

inline bool
operator==(const File::Offset &one, const File::Offset &two)
{
    return one.chunk == two.chunk &&
            one.offsetInChunk == two.offsetInChunk;
}

inline bool
operator>=(const File::Offset &one, const File::Offset &two)
{
    return one.chunk > two.chunk ||
            (one.chunk == two.chunk && one.offsetInChunk >= two.offsetInChunk);
}

inline bool
operator>(const File::Offset &one, const File::Offset &two)
{
    return two < one;
}

inline bool
operator<=(const File::Offset &one, const File::Offset &two)
{
    return two >= one;
}


} /* namespace trace */

#endif
