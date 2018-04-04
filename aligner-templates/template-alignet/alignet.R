## ----,warning=FALSE------------------------------------------------------
library(AligNet)

make.clusters <- function(net, blast.net) {
  ## ----matrices------------------------------------------------------------
  blast = matrix(0, nrow = vcount(net), ncol = vcount(net))

  ## ----adapt---------------------------------------------------------------
  dimnames(blast) = list(V(net)$name,V(net)$name)
  protsr = intersect(V(net)$name, rownames(blast.net))
  protsc = intersect(V(net)$name, colnames(blast.net))
  for(p in protsr){
    blast[p,protsc] = blast.net[p,protsc]
  }
  rm(protsr)
  rm(protsc)
  rm(blast.net)

  ## ----clusters------------------------------------------------------------
  blast = blast/max(blast)
  sigma = (as.matrix(blast) + compute.matrix(net))/2
  rm(blast)
  q3 = fivenum(sigma)[3]
  clust = cluster.network(sigma,q3,20)
  rm(sigma)
  clusters = extract.clusters(net,clust)
  rm(clust)
  names(clusters)=V(net)$name

  return(clusters)
}

## ----networks------------------------------------------------------------
net1 = read.network("net1.tab",mode="edges",sep="\t")
blast.net1 = read.matrix("blast-net1.tab",mode="col3")
clusters1 = make.clusters(net1, blast.net1)
rm(blast.net1)

net2 = read.network("net2.tab",mode="edges",sep="\t")
blast.net2 = read.matrix("blast-net2.tab",mode="col3")
clusters2 = make.clusters(net2, blast.net2)
rm(blast.net2)

## ----localalign----------------------------------------------------------

blast.net1.net2 = read.matrix("blast-net1-net2.tab",mode="col3")
blast = matrix(0, nrow = vcount(net1), ncol = vcount(net2))
dimnames(blast) = list(V(net1)$name,V(net2)$name)
prots1 = intersect(V(net1)$name, rownames(blast.net1.net2))
prots2 = intersect(V(net2)$name, colnames(blast.net1.net2))
for(p1 in prots1){
  blast[p1,prots2] = blast.net1.net2[p1,prots2]
}
rm(prots1)
rm(prots2)
rm(blast.net1.net2)

blast = blast/max(blast)

numCores = Sys.getenv('ALIGNET_NUM_THREADS')
numCores = ifelse(numCores == '', 1, as.integer(numCores))
localAligns = align.local.all(clusters1,clusters2,blast,0,cores=numCores,1-blast)

rm(clusters1)
rm(clusters2)

## ----globalalign---------------------------------------------------------
global = align.global(localAligns,blast)

#save(global, file='global.Rdata')
write.table(global[[2]], file='alignment-net1-net2.tab', col.names=FALSE, sep='\t')
data(go)

## ----scores--------------------------------------------------------------
scores = data.table(
  ec = EC.score(global[[2]], net1, net2),
  fc = FC.score(global[[2]], go))

write.table(scores, file='alignment-net1-net2-scores.tab', col.names=TRUE, sep='\t')
