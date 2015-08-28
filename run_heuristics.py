import os
from os.path import join, abspath, dirname
import sys
import time
import logging
import pprint
import cPickle
import re
import csv
import subprocess
import argparse
import ConfigParser
import timeit
import string

heuristic_list = [ "halfway", "newadaptive", "statusquo", "lazy", "dynamic", "aggressive", ]

# TODO put into config file
dacapo_path = "./dacapo-9.12-bach.jar"
specjvm_path = "./specjvm2008/SPECjvm2008.jar"


def setup_logger( logger_name = None,
                  filename = "run_heuristics.log",
                  targetdir = ".",
                  debugflag = 0 ):
    assert( logger_name != None )
    # Set up main logger
    logger = logging.getLogger( logger_name )
    formatter = logging.Formatter( '[%(funcName)s] : %(message)s' )
    filehandler = logging.FileHandler( os.path.join( targetdir, filename ) , 'w' )
    if debugflag:
        logger.setLevel( logging.DEBUG )
        filehandler.setLevel( logging.DEBUG )
    else:
        filehandler.setLevel( logging.ERROR )
        logger.setLevel( logging.ERROR )
    filehandler.setFormatter( formatter )
    logger.addHandler( filehandler )
    return logger

def create_directories( blist ):
    """This will error if WORK exists.
    Then this creates the WORK directory and the benchmark directories
    there. When create_directories returns, it wil lbe in the WORK directory."""
    # TODO Make this configurable
    dirname = "WORK"
    if os.path.exists(dirname):
        print "%s directory exists. Please rename and try again." % dirname
        exit(10)
    os.mkdir(dirname)
    os.chdir(dirname)
    for bmark in blist:
        os.mkdir(bmark)
    
def write_csvfile( tgtpath = None,
                   data = None,
                   header = None,
                   pp = None,
                   logger = None ):
    with open(tgtpath, 'wb') as fp:
        cw = csv.writer( fp,
                         quotechar = '"',
                         quoting = csv.QUOTE_NONNUMERIC )
        cw.writerow( header )
        for csvrow in data:
            assert(len(csvrow) == 9)
            result_list = pp.pformat( csvrow[-1] )
            result_list = result_list.replace("[", "")\
                                     .replace("]", "")\
                                     .replace(" ", "")\
                                     .replace(",", ";")
            csvrow.pop()
            csvrow.append(result_list)
            cw.writerow(csvrow)

def construct_row( benchmark = None,
                   runtime_list = None,
                   gc_algo = None,
                   heuristic = None,
                   min_heap = None,
                   max_heap = None,
                   par_gcthreads = None,
                   conc_gcthreads = None,
                   number_iterations = None,
                   drop_warmup = True ):
    return [ benchmark,
             gc_algo,
             heuristic,
             min_heap,
             max_heap,
             par_gcthreads,
             conc_gcthreads,
             (number_iterations - 1) if drop_warmup else number_iterations,
             runtime_list[1:] if drop_warmup else runlist[:] ]

def run_benchmark( benchmark = None,
                   gc_algo = "shenandoah",
                   number = None,
                   specjvm_flag = False,
                   dacapo_flag = False,
                   java_actual_path = None,
                   dacapo_path = None,
                   specjvm_path = None,
                   heuristic = None,
                   min_heap = "2g",
                   max_heap = "2g",
                   appnum = 1,
                   par_gcthreads = 6,
                   conc_gcthreads = 2,
                   perf = False,
                   printgcdetails = False,
                   fake = False,
                   logger = None,
                   pp = None ):
    assert( type(number) == type(int(0)) )
    assert( heuristic != None )
    assert( dacapo_flag or specjvm_flag )
    if dacapo_flag and specjvm_flag:
        print "WARNING: Benchmark %s found in both dacapo and specjvm. Defaulting to DaCapo."
        specjvm_flag = False
    print "==========================================================================="
    os.chdir( benchmark )
    min_heap_label = min_heap if not (min_heap == None) else "None"
    max_heap_label = max_heap if not (max_heap == None) else "None"
    gc_logfile = "%s-%s-%s-min%s-max%s-p%d-c%d-bt%d-gc.log" % \
        ( benchmark, gc_algo,  heuristic, min_heap_label, max_heap_label, par_gcthreads, conc_gcthreads, appnum )
    if not fake:
        gc_stdout = "%s-%s-%s-min%s-max%s-p%d-c%d-bt%d-gc-output.txt" % \
            ( benchmark, gc_algo, heuristic, min_heap_label, max_heap_label, par_gcthreads, conc_gcthreads, appnum )
    else:
        gc_stdout = "/dev/null"
    print "gc_stdout", gc_stdout
    with open(gc_stdout, "w") as fptr:
        cmd = [ java_actual_path,
                "-XX:ParallelGCThreads=%d" % par_gcthreads,
                "-XX:ConcGCThreads=%d" % conc_gcthreads ]
        if min_heap != None:
            cmd.extend( [ "-Xms%s" % min_heap ] )
        if max_heap != None:
            cmd.extend( [ "-Xmx%s" % max_heap ] )
        # Add options to select GC algorithm
        if gc_algo == "shenandoah":
            cmd.extend( [ "-XX:+UseShenandoahGC", 
                          "-XX:ShenandoahGCHeuristics=%s" % heuristic ] )
        elif gc_algo == "g1":
            cmd.append( "-XX:+UseG1GC" )
        else:
            assert( gc_algo == "defaultgc" )
            # Run with the default collector for the java being used.
        # Add debug flags if needed
        if printgcdetails:
            cmd.extend( [ "-XX:+PrintGCDetails",
                          "-XX:+PrintGCTimeStamps",
                          "-XX:+PrintGC", 
                          "-Xloggc:%s" % gc_logfile ] )

        # Where the benchmark application is
        if dacapo_flag:
            cmd.extend( [ "-jar",
                          dacapo_path,
                          benchmark,
                          "-n%d" % number ] )
        elif specjvm_flag:
            specjvm_dirname = os.path.dirname( specjvm_path )
            cmd.extend( [ "-Dspecjvm.home.dir=%s" % specjvm_dirname,
                          "-jar", specjvm_path,
                          "-ikv", # Skip verification.
                          "-ict", # Ignore check test.
                          "-bt", "%d" % appnum, # App thread number
                          "--iterations", "200",
                          benchmark ] )

        if fake:
            print "CMD:", cmd
        else:
            print "CMD:", cmd
            javaproc = subprocess.Popen( cmd,
                                         stdout = subprocess.PIPE,
                                         stdin = subprocess.PIPE,
                                         stderr = subprocess.PIPE )
            result = javaproc.communicate()
            fptr.writelines( result )
    os.chdir( ".." )
    return benchmark

def set_benchmark_flags( config ):
    dacapo_flag = len(config["dacapo_benchmarks"]) > 0
    specjvm_flag = len(config["specjvm_benchmarks"]) > 0
    return (dacapo_flag, specjvm_flag)

def main_process( config = None,
                  gc_algo = "shenandoah",
                  min_heap = "2g",
                  max_heap = "2g",
                  output = None,
                  java_actual_path = None,
                  number = 5,
                  number_appthreads = None,
                  pargcthreads = 2,
                  concgcthreads = 2,
                  printgcdetails = False,
                  debugflag = False,
                  logger = None,
                  heuristic = None,
                  fake = False,
                  pp = None ):
    global heuristic_list
    java_actual_path = os.path.abspath( java_actual_path )
    if concgcthreads < 2:
        print "ConcGCThreads must be >= 2."
        exit(2)
    # Loop through required benchmarks
    dacapo_benchmark_list = config["dacapo_benchmarks"]
    specjvm_benchmark_list = config["specjvm_benchmarks"]
    blist = dacapo_benchmark_list + specjvm_benchmark_list
    pp.pprint(blist)
    (dacapo_flag, specjvm_flag) = set_benchmark_flags( config )
    create_directories( blist )
    # Set benchmark paths
    dacapo_path = config["dacapo_path"]
    if not os.path.isfile(dacapo_path):
        print "Can not find dacapo in: %s" % dacapo_path
        dacapo_flag = False
    specjvm_path = config["specjvm_path"]
    if not os.path.isfile(specjvm_path):
        print "Can not find specjvm in: %s" % specjvm_path
        specjvm_flag = False
    if not specjvm_flag and not dacapo_flag:
        print "No benchmarks to run! Exiting"
        print "base path   : %s" % prefix
        print "java path   : %s" % java_actual_path
        print "dacapo path : %s" % dacapo_path
        print "specjvm path: %s" % specjvm_path
        print "benchmark list: %s" % pp.pformat(blist)
        # ERROR No benchmarks to run.
        # Either, no valid benchmarks specified or the paths don't exist.
        exit(44)
    print "==========================================================================="
    if gc_algo == "defaultgc":
        print "-------------> USING DEFAULT GC!!! <---------------------------------------"
    elif gc_algo == "g1":
        print "-------------> USING G1 GC!!! <--------------------------------------------"
    else:
        print "-------------> USING SHENANDOAH GC!!! <------------------------------------"
    print "==========================================================================="
    rows = []
    for bmark in blist:
        if heuristic == "ALL":
            actual_hlist = heuristic_list if gc_algo == "shenandoah" else [ "None" ]
        else:
            assert( heuristic in heuristic_list )
            actual_hlist = [ heuristic ]
        # Loop through all heuristics
        for heuristic in actual_hlist:
            # TODO TODO TODO TODO
            # for parnum in xrange(1, pargcthreads + 1):
            # TODO: Do we want parnum hardcoded or not?
            for concnum in xrange(2, concgcthreads + 1):
                parnum = 2
                for appnum in xrange(1, number_appthreads):
                    # Run benchmark
                    benchmark = run_benchmark( benchmark = bmark,
                                               java_actual_path = java_actual_path,
                                               specjvm_flag = (bmark in specjvm_benchmark_list),
                                               dacapo_flag = (bmark in dacapo_benchmark_list),
                                               dacapo_path = dacapo_path,
                                               specjvm_path = specjvm_path,
                                               number = number,
                                               gc_algo = gc_algo,
                                               heuristic = heuristic,
                                               min_heap = min_heap,
                                               max_heap = max_heap,
                                               par_gcthreads = parnum,
                                               conc_gcthreads = concnum,
                                               appnum = appnum,
                                               printgcdetails = printgcdetails,
                                               fake = fake,
                                               logger = logger,
                                               pp = pp )
                    print "---------------------------------------------------------------------------"
                    if True: # TODO TODO Check for failed here.
                        # Right now we're not really checking if it passed or failed.
                        # It would obviously have to be different for Dacapo vs SpecJVM.
                        pass
                    else:
                        logger.debug( "Benchmark %s with %s - %s - FAILED." %
                                      (bmark, gc_algo, str(heuristic)) )
    logger.error( "=====[ DONE ]==============================================================" )
    print "=====[ DONE ]=============================================================="
    exit(0)

def config_section_map( section, config_parser ):
    result = { "dacapo_benchmarks" : [],
               "specjvm_benchmarks" : []  }
    options = config_parser.options(section)
    for option in options:
        if option == "dacapo_benchmarks":
            temp = config_parser.get(section, "dacapo_benchmarks")
            benchmarks = re.sub( r'\s+', "", temp )
            # try:
            benchlist = benchmarks.split(",")
            # except:
            #     print "Unable to parse DaCapo benchmark option: %s" % str(temp)
            #     exit(3)
            result["dacapo_benchmarks"].extend( benchlist )
        elif option == "specjvm_benchmarks":
            temp = config_parser.get(section, "specjvm_benchmarks")
            benchmarks = re.sub( r'\s+', "", temp )
            # try:
            benchlist = benchmarks.split(",")
            # except:
            #     print "Unable to parse option: %s" % str(temp)
            #     exit(3)
            result["specjvm_benchmarks"].extend( benchlist )
        elif option == "dacapo_path":
            result["dacapo_path"] = config_parser.get(section, "dacapo_path")
        elif option == "specjvm_path":
            result["specjvm_path"] = config_parser.get(section, "specjvm_path")
    return result

def process_config( args ):
    global pp
    assert( args.config != None )
    print "CONFIG."
    config_parser = ConfigParser.ConfigParser()
    config_parser.read( args.config )
    config = config_section_map( "global", config_parser )
    # pp.pprint(config)
    return config

def __main():
    global benchmark_list
    pp = pprint.PrettyPrinter( indent = 4 )
    # Loop through required benchmarks
    # set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument( "--config",
                         help = "Specify configuration filename.",
                         action = "store",
                         default = None )
    parser.add_argument( "output",
                         help = "Output CSV file for results." )
    parser.add_argument( "--javapath",
                         help = "Specify java location.",
                         action = "store",
                         default = None )
    parser.add_argument( "--num",
                         help = "Number of iterations per benchmark.",
                         action = "store",
                         default = 5 )
    parser.add_argument( "--pargcthreads",
                         help = "Number of parallel GC threads as supplied to the -XX:ParallelGCThreads option.",
                         action = "store",
                         default = 2 )
    parser.add_argument( "--concgcthreads",
                         help = "Number of concurrent GC threads as supplied to the -XX:ConcGCThreads option.",
                         action = "store",
                         default = 2 )
    parser.add_argument( "--appthreads",
                         help = "Number of benchmark application threads.",
                         action = "store",
                         default = 2 )
    parser.add_argument( "--xms",
                         help = "Min heap parameter to be passed to -Xms",
                         action = "store",
                         default = None )
    parser.add_argument( "--xmx",
                         help = "Max heap parameter to be passed to -Xmx",
                         action = "store",
                         default = None )
    parser.add_argument( "--printgcdetails",
                         help = "Enable PrintGCDetails.",
                         action = "store_true",
                         default = False )
    parser.add_argument( "--shenandoah",
                         help = "Use Shenandoah GC.",
                         action = "store_true",
                         default = False )
    parser.add_argument( "--defaultgc",
                         help = "Use defualt GC. Mutually exclusive with --g1 and --shenandoah.",
                         action = "store_true",
                         default = False )
    parser.add_argument( "--g1",
                         help = "Use G1 GC. Mutually exclusive with --defaultgc and --shenandoah.",
                         action = "store_true",
                         default = False )
    parser.add_argument( "--heuristic",
                         help = "Specify heuristic to use.",
                         action = "store",
                         default = "newadaptive" )
    parser.add_argument( "--debug",
                         help = "Enable debug output.",
                         action = "store_true",
                         default = False )
    parser.add_argument( "--fake",
                         help = "Don't run. Just print out the commands to run.",
                         action = "store_true",
                         default = False )
    parser.add_argument( "--logfile", help = "Specify logfile name.",
                         action = "store", default = None )
    parser.add_argument( "--version", help = "Version number. Default is 1",
                         action = "store", default = 1 )
    parser.add_argument( "--testjava",
                         help = "Test the java executable only.",
                         action = "store_true",
                         default = False )
    args = parser.parse_args()
    # Get java path
    if args.javapath != None:
        java_actual_path = args.javapath 
    else:
        parser.error("Please provide a --javapath.")
    if not os.path.isfile(java_actual_path):
        parser.error("Invalid --javapath: %s" % str(java_actual_path))

    if args.testjava:
        cmd = [ java_actual_path, "-version" ]
        print "Testing java:", cmd
        javaproc = subprocess.Popen( cmd,
                                     stdout = subprocess.PIPE,
                                     stdin = subprocess.PIPE,
                                     stderr = subprocess.PIPE )
        result = javaproc.communicate()
        for x in result:
            print x
        exit(0)

    # Check config file
    if args.config != None:
        config = process_config( args )
    else:
        parser.error("Please provide a configuration file using --config.")

    # Determine GC algorithm
    shenandoah = args.shenandoah
    g1 = args.g1
    defaultgc =  args.defaultgc
    if ( (shenandoah and g1) or
         (shenandoah and defaultgc) or
         (g1 and defaultgc) or
         not (shenandoah or g1 or defaultgc) ):
         print "Invalid selection of GC algorithm. Please select just one of --shenandoah, --g1 or --defaultgc."
         exit(2)
    gc_algo = "shenandoah" if shenandoah else ("g1" if g1 else "defaultgc")

    # Get logfile name
    logfile = args.logfile if args.logfile != None else \
        ("run_heuristics.log")

    # Setup logging
    logger = setup_logger( filename = logfile,
                           logger_name = 'run_heuristics',
                           debugflag = args.debug )
    #
    # Main processing
    #
    return main_process( config = config,
                         output = args.output,
                         java_actual_path = java_actual_path,
                         number = int(args.num),
                         number_appthreads = args.appthreads,
                         pargcthreads = int(args.pargcthreads),
                         concgcthreads = int(args.concgcthreads),
                         gc_algo = gc_algo,
                         min_heap = args.xms,
                         max_heap = args.xmx,
                         printgcdetails = args.printgcdetails,
                         heuristic = args.heuristic,
                         debugflag = args.debug,
                         logger = logger,
                         fake = args.fake,
                         pp = pp )

if __name__ == "__main__":
    __main()
