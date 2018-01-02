/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.hadoop;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;

/**
 * A collection of method that simplify the generation of cryptographic Digests
 */
public final class Digester {

    private final static String ENCODING = "ascii";
    private final static String DIGEST_ALGORITHM = "MD5";

    private final static BigInteger LSB_MASK = new BigInteger(new byte[] {
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255,
            (byte) 255 });

    /**
     * Generate a MD5 digest from the given content
     *
     * @param aContent
     *            a {@code byte[]} array
     * @return an {@code byte[]} array of size 16 (128 bits)
     * @throws DigesterException
     *             upon failed cryptographic operation
     */
    public final static byte[] digestMD5(byte[] aContent) throws DigestException {
        if (aContent == null)
            return null;

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(DIGEST_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new DigestException("generating signature", e);
        }
        md.reset();
        return md.digest(aContent);
    }

    /**
     * Generate a Base64 representation of the MD5 digest for the given content
     *
     * @param aContent
     *            a {@code byte[]} array
     * @return a {@code Base64} representation of the computed MD5 digest
     * @throws DigesterException
     *             upon failed cryptographic and encoding operations
     */
    public final static String digestMD5asBase64(byte[] aContent) throws DigestException {
        if (aContent == null)
            return null;

        String encoded = null;
        try {
            encoded = new String(Base64.encodeBase64(digestMD5(aContent)), ENCODING);
        } catch (UnsupportedEncodingException encex) {
            throw new DigestException("generating signature", encex);
        }
        return encoded;
    }

    /**
     * Generate a {@code BigInteger} representation of the MD5 digest for the given content
     *
     * @param aContent
     *            a content {@code String}
     * @return a {@code BigInteger} representation of the computed MD5 digest
     * @throws DigesterException
     *             upon failed cryptographic operation
     */
    public final static BigInteger digestMD5asBigInteger(String aContent) throws DigestException {
        if (aContent == null)
            return null;

        return new BigInteger(1, digestMD5(aContent.getBytes()));
    }

    /**
     * Generate a {@code UUID} representation of the MD5 digest for the given content. This is possible because MD5
     * digests and UUIDs are both 128 bit long
     *
     * @param aContent
     *            a content {@code String}
     * @return a {@code UUID} representation of the computed MD5 digest
     * @throws DigesterException
     *             upon failed cryptographic operation
     */
    public final static UUID digestMD5asUUID(String aContent) throws DigestException {
        if (aContent == null)
            return null;

        BigInteger bi = digestMD5asBigInteger(aContent);
        return new UUID(bi.shiftRight(64).longValue(), bi.and(LSB_MASK).longValue());
    }

    /**
     * Generate a Base64 representation of the MD5 digest for the given content
     *
     * @param aContent
     *            a content {@code String}
     * @return a {@code Base64} representation of the computed MD5 digest
     * @throws DigesterException
     *             upon failed cryptographic and encoding operations
     */
    public final static String digestMD5asBase64(String aContent) throws DigestException {
        if (aContent == null)
            return null;

        return digestMD5asBase64(aContent.getBytes());
    }

    public final static class DigestException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public DigestException() {
            super();
        }
        public DigestException(String message, Throwable cause) {
            super(message, cause);
        }
        public DigestException(String message) {
            super(message);
        }
        public DigestException(Throwable cause) {
            super(cause);
        }
    }
}