from env import *

import patricia

# Work as input to update analysis functions
class Period():

    def __init__(self, index):
        self.sdate = daterange[index][0] 
        self.edate = daterange[index][1] 
        
        # XXX this is no good. should depends on the monitor list
        # XXX should be flexible cause we may change the co set future
        self.co_list = list()
        for co in all_collectors.keys():
            if int(all_collectors[co]) <= int(self.sdate):
                self.co_list.append(co)

        # location to store supporting files
        self.spt_dir = datadir + 'support/' + str(index) + '/'

        # Note: do not change this
        self.rib_info_file = rib_info_dir + sdate + '_' + edate + '.txt'
    
        self.monitors = list() # Or use a trie?
        self.prefixes = patricia.trie(None)

    # Note: we can only get the *latest* AS to nation mapping
    def get_as2nation_file(self):
        print 'Calculating AS to nation file...'
        if os.path.exists(pub_spt_dir+'as2nation.txt'):
            return 0

        the_url = 'http://bgp.potaroo.net/cidr/autnums.html'
        rows = cmlib.get_weblist(the_url)
        rows = rows.split('\n')
        f = open(pub_spt_dir+'as2nation.txt', 'w')
        for line in rows:
            if 'AS' not in line:
                continue
            nation = line.split(',')[-1] 
            ASN = line.split()[0].strip('AS')
            f.write(ASN+' '+nation+'\n')
        f.close()

        return 0

    def get_global_monitor(self):
        # The monitor info has already been stored when pre-processing the data
        # jusy read the existent files
        # filter out the ones with limited view
        # TODO get the file's location according to the name and location of the RIB file
        return 0

    def get_prefix(self):
        return 0

    #TODO select monitors; build a trie;  output monitor info
    #TODO select prefixes in two ways
    #TODO download support files and build it in memo if necessary

