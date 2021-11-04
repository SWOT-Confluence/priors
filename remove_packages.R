### RSCRIPT TO REMOVE INSTALLED R PACKAGES
### Taken and adapter from: 
### https://www.r-bloggers.com/2016/10/how-to-remove-all-user-installed-packages-in-r/

# create a list of all installed packages
installed <- as.data.frame(installed.packages())
# do not remove base or recommended packages either
installed <- installed[!(installed[,"Priority"] %in% c("base", "recommended")),]
# determine the library where the packages are installed
path.lib <- unique(installed$LibPath)
# create a vector with all the names of the packages to remove
pkgs.to.remove <- installed[,1]
# remove the packages
sapply(pkgs.to.remove, remove.packages, lib = path.lib)