#ifndef TRACE_COMPRESSION_LIBRARY_HPP
#define TRACE_COMPRESSION_LIBRARY_HPP

#include <assert.h>

namespace trace {


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

    virtual void compress(const char *src, size_t inputLength, char *dst, size_t *outLength);
    virtual void uncompress(const char *src, size_t inputLength, char *dst);
    virtual size_t maxCompressedLength(size_t inputLength);
    virtual size_t uncompressedLength(const char *src, size_t inputLength);

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

    virtual void compress(const char *src, size_t inputLength, char *dst, size_t *outLength);
    virtual void uncompress(const char *src, size_t inputLength, char *dst);
    virtual size_t maxCompressedLength(size_t inputLength);

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

} //end namespace trace

#endif
