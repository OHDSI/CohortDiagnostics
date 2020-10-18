install.packages(pkgs = 'magrittr', dependencies = TRUE)
install.packages(pkgs = 'remotes', dependencies = TRUE)
remotes::install_github(repo = c('OHDSI/PhenotypeLibrary', 'OHDSI/OhdsiSharing', 'OHDSI/ROhdsiWebApi'), dependencies = TRUE, upgrade = TRUE)
