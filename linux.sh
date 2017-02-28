TGZ=/tmp/kfk.tgz
RPO=kfk
tar czvf $TGZ Package.swift Sources Tests 
scp $TGZ nut:/tmp
ssh nut "cd /tmp;rm -rf $RPO;mkdir $RPO; cd $RPO; tar xzvf $TGZ;swift build;swift test"
