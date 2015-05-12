AC_INIT([spider_service], [1.0.1.0], [yanyong@founder.com.cn])

AM_INIT_AUTOMAKE([foreign])
AC_PROG_CXX
AC_LANG([C++])
AC_PROG_LIBTOOL

BITS=`if [[ 64 -eq $(getconf LONG_BIT) ]]; then echo -n $(getconf LONG_BIT); fi`
AC_SUBST(BITS)

boost_path=/usr/local
AC_SUBST(boost_path)
AC_ARG_WITH([boost_path],
	AS_HELP_STRING([--with-boost_path=PATH],[which should have 2 folders bellow: include lib (default is /usr/local)]),
	    [
	    	if test "x$withval" != "x";then
			boost_path="$withval"
		fi
	    ],
	    [])

common_path=$PWD/common
AC_SUBST(common_path)
AC_ARG_WITH([common_path],
	AS_HELP_STRING([--with-common_path=PATH],[default is ./common]),
	    [
	    	if test "x$withval" != "x";then
			common_path="$withval"
		fi
	    ],
	    [])

svn_info="NONE"
if svn info &>/dev/null; then
    info=`svn info`
    svn_info=`echo "$info" | awk '/URL/{print $NF}'`"@r"`echo "$info" | awk '/Last Changed Rev/{print $NF}'`
fi

AC_ARG_WITH([release],
        AS_HELP_STRING([--with-release],
            [use optimize (default is NO)]),
        [
        if test "$withval" = "yes"; then
        CXXFLAGS="-g -O2 -finline-functions -fno-strict-aliasing -Wall -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -DJU_64BIT -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DNDEBUG -DTAIR_SVN_INFO=\\\"${svn_info}\\\""
        CFLAGS="-g -O2 -finline-functions -fno-strict-aliasing -Wall -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -DJU_64BIT -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DNDEBUG -DTAIR_SVN_INFO=\\\"${svn_info}\\\""
        fi
        ],
        [
        CXXFLAGS="-g -Wall -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -DJU_64BIT -D_LARGE_FILE  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DTAIR_DEBUG -DTAIR_SVN_INFO=\\\"${svn_info}\\\""
        CFLAGS="-g -Wall -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -DJU_64BIT -D_LARGE_FILE  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DTAIR_DEBUG -DTAIR_SVN_INFO=\\\"${svn_info}\\\""
        ]
        )

GCOV_LIB=
AC_SUBST(GCOV_LIB)
AC_ARG_WITH([profile],
            AS_HELP_STRING([--with-profile],
                [enable code coverage(default is NO)]),
            [
            if test "$withval" = "yes"; then
            CXXFLAGS="-g -fprofile-arcs -ftest-coverage ${CXXFLAGS}"
            GCOV_LIB=-lgcov
            fi
            ])

AC_CONFIG_FILES([\
        Makefile
         ])
AC_OUTPUT
