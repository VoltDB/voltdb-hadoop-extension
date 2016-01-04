/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.hadoop

import static org.voltdb.VoltType.*

import org.apache.commons.cli.MissingArgumentException
import org.apache.commons.cli.UnrecognizedOptionException
import org.apache.hadoop.mapred.JobConf
import org.voltdb.VoltType
import org.voltdb.hadoop.mapred.VoltLoader;

import spock.lang.Specification

class VoltConfigurationSpec extends Specification {

    static String THINGS = "THINGS"
    static VoltType [] COLUMNTYPES = [INTEGER,BIGINT,FLOAT,STRING,TIMESTAMP,VARBINARY] as VoltType[]

    def setupSpec() {
        VoltConfiguration.typesFor(THINGS, COLUMNTYPES)
        DataAdapters.adaptersFor(THINGS, COLUMNTYPES)
    }

    def "correct command line options are processed correctly"() {
        given:
            def opts = new LoaderOpts(args as String[])
        expect:
            with(opts) {
                user     == utente
                password == wordpass
                servers  == hosts as String []
                source   == origin
                table    == matrix
            }
        where:
            utente | wordpass  |  origin  |  matrix  | hosts         | args
            'j1hn' | 'pw1'     | 'source' | 'things' | ['uno','due'] | ['-s','uno,due','-u','j1hn','-p','pw1','source','things']
            'j2hn' | 'pw2'     | 'source' | 'things' | ['uno','due'] | ['-s','uno, ','due','-u','j2hn','-p','pw2','source','things']
            'j3hn' | 'pw3'     | 'source' | 'things' | ['uno','due'] | ['-s','uno,',',',',,,','due','-u','j3hn','-p','pw3','source','things']
            null   | null      | 'source' | 'things' | ['localhost'] | ['source','things']
            'j4hn' | 'pw4'     | 'source' | 'things' | ['uno','due'] | ['--servers', 'uno,due','--user','j4hn','--p','pw4','source','things']
            null   | null      | 'source' | 'things' | ['uno']       | ['source','things','--servers','uno']
    }

    def "rejects invalid command line options"() {
        when:
            new LoaderOpts(args as String[])
        then:
            def e = thrown(exception)
            e.cause?.getClass() == cause
        where:
            exception                | cause                       | args
            IllegalArgumentException | null                        | []
            IllegalArgumentException | null                        | ['source']
            IllegalArgumentException | null                        | ['-s','uno,due','-u','john','-p','please']
            RuntimeException         | UnrecognizedOptionException | ['-s','uno,due','-u','john','-q','please','source','things']
            RuntimeException         | MissingArgumentException    | ['-s','-u','john','-p','please','source','things']
            RuntimeException         | MissingArgumentException    | ['-u','-p','please','source','things']
            RuntimeException         | MissingArgumentException    | ['source','things','-p']
            IllegalArgumentException | null                        | ['-p','source','things']
    }

    def "configures JObConf as expected"(){
        given:
            def hconf = new LoaderOpts(args as String[]).configure(new JobConf(VoltLoader.class))
            def vconf = new VoltConfiguration(hconf)
        expect:
            with(vconf) {
                userName  == user
                password  == wordpass
                hosts     == servers
                tableName == table
                minimallyConfigured
                tableColumnTypes == types
            }
        where:
            user | wordpass | servers       | table  | types       | args
            'jim'| 'please' | ['uno','due'] | THINGS | COLUMNTYPES |  ['-s','uno,due','-u','jim','-p','please','source',THINGS]
    }
}
