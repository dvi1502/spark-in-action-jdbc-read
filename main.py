import runpy
import sys



if __name__ == '__main__':
    # sys.argv[1:] = ['foo', 'bar']
    #runpy.run_module('./lab300_advanced_queries/mySQLWithWhereClauseToDatasetApp.py', run_name='__main__', alter_sys=True)
    runpy.run_path('./lab300_advanced_queries/mySQLWithWhereClauseToDatasetApp.py', run_name='__main__')
    # module_run('./lab300_advanced_queries/mySQLWithWhereClauseToDatasetApp.py')

