repo = getOption("repos")
# set up the server from which you will download the package.
repo["CRAN"] = "http://cran.case.edu"
options(repos = repo)
rm(repo)

install.packages('devtools', verbose=TRUE)
install.packages('igraph', verbose=TRUE)
install.packages('data.table', verbose=TRUE)
install.packages('parallel', verbose=TRUE)
install.packages('plot3D', verbose=TRUE)
install.packages('clue', verbose=TRUE)
install.packages('plyr', verbose=TRUE)
install.packages('lpSolveAPI', verbose=TRUE)
library(devtools)
install_github("adriaalcala/AligNet", build_vignettes=TRUE)
