for chrom in range(1, 20):
    for i in range(100000):
        print "chr%s\t%i\t%i" % (chrom, i, i + 40)
    for x in range(10):
        print "chr%s\t%i\t%i" % (chrom, i + 50, i + 60)
