/******************************************************************************
 System:    Reyes Holdings ETL Automation
 Package:   Lib-Rails
 Class:     Converter
 Purpose:   To be used in business rules expressions
 Helps to convert between different data types safely
 ******************************************************************************/

package com.rh.bi.etl.spark;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import static java.lang.Long.*;


public final class MD5HashSha1 extends UDF {
    public long evaluate(final Text s) {
        if (s == null || s.toString().isEmpty() ) { return 0; }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            byte[] bytes = s.getBytes();
            byte[] digest = md.digest(bytes);
            String hexString = "";
            for (int i = digest.length - 8; i < digest.length; i++) //grab the lo 8 bytes
            {
                if ((0xff & digest[i]) < 0x10) //make sure we capture a full byte
                {
                    hexString+="0" + Integer.toHexString((0xFF & digest[i]));
                }
                else
                {
                    hexString+=Integer.toHexString(0xFF & digest[i]);
                }
            }
            return parseUnsignedLong(hexString,16);
        } catch (NoSuchAlgorithmException e) {
            return 0;
        }    }
}

