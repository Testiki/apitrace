#include <snappy.h>
#include <lz4.h>
#include <lz4hc.h>
#include <zlib.h>

#include "trace_compression_library.hpp"

using namespace trace;

void SnappyLibrary::compress(const char *src, size_t inputLength, char *dst, size_t *outLength)
{
    ::snappy::RawCompress(src, inputLength, dst, outLength);
}

void SnappyLibrary::uncompress(const char *src, size_t inputLength, char *dst)
{
    ::snappy::RawUncompress(src, inputLength, dst);
}

size_t SnappyLibrary::maxCompressedLength(size_t inputLength)
{
    return ::snappy::MaxCompressedLength(inputLength);
}

size_t SnappyLibrary::uncompressedLength(const char *src, size_t inputLength)
{
    size_t result;
    ::snappy::GetUncompressedLength(src, inputLength, &result);
    return result;
}


void LZ4Library::compress(const char *src, size_t inputLength, char *dst, size_t *outLength)
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

void LZ4Library::uncompress(const char *src, size_t inputLength, char *dst)
{
    LZ4_uncompress_unknownOutputSize(src + m_lengthSize, dst, inputLength - m_lengthSize, m_chunkSize);
}

size_t LZ4Library::maxCompressedLength(size_t inputLength)
{
    return LZ4_compressBound(inputLength) + m_lengthSize;
}

