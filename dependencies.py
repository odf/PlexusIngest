from modulefinder import ModuleFinder

finder = ModuleFinder(excludes = ['numpy', 'PIL', 'yaml'])
finder.run_script('update_plexus.py')
finder.report()
