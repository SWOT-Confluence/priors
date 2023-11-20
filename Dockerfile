# STAGE 0 - Ubuntu and R packages (geoBAM)
FROM rpy2/base-ubuntu:master-20.04 as stage0
RUN echo "America/New_York" | tee /etc/timezone \
	&& apt update \
	&& apt upgrade -y \
	&& DEBIAN_FRONTEND=noninteractive apt install -y \
		build-essential \
		curl \
		gcc \
		gfortran \
        locales \
		libcurl4-gnutls-dev \
		libfontconfig1-dev \
		libfribidi-dev \
		libgit2-dev \
		libharfbuzz-dev \
		libnetcdf-dev \
		libnetcdff-dev \
		libssl-dev \
		libtiff5-dev \
		libxml2-dev \
		tzdata \
    && locale-gen en_US.UTF-8 \
	&& /usr/bin/curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

# Stage 2 - R and R packages (geoBAM)
FROM stage0 as stage1
COPY remove_packages.R /app/remove_packages.R
RUN apt -y install \
		software-properties-common \
		dirmngr \
	&& . /etc/lsb-release \
	&& wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc \
	&& add-apt-repository -y "deb https://cloud.r-project.org/bin/linux/ubuntu $(lsb_release -cs)-cran40/" \
	&& /usr/bin/Rscript /app/remove_packages.R \
	&& /usr/bin/Rscript -e 'Sys.setenv(DOWNLOAD_STATIC_LIBV8 = 1); install.packages("V8")' \
    && /usr/bin/Rscript -e "install.packages('dplyr', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
    && /usr/bin/Rscript -e "install.packages('reshape2', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
    && /usr/bin/Rscript -e "install.packages('settings', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
    && /usr/bin/Rscript -e "install.packages('devtools', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('RNetCDF', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('foreach', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('doParallel', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('yaml', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('data.table', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('rjson', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
    && /usr/bin/Rscript -e "install.packages('R.utils', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('BBmisc', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&& /usr/bin/Rscript -e "install.packages('tidyhydat', dependencies=TRUE, repos='http://cran.rstudio.com/')" \
	&&/usr/bin/Rscript -e "install.packages('RSelenium', dependencies=TRUE, repos='http://cran.rstudio.com/')"\
	&&/usr/bin/Rscript -e "install.packages('rvest', dependencies=TRUE, repos='http://cran.rstudio.com/')"

#Stage 3 - Python packages
FROM stage1 as stage2
COPY requirements.txt /app/requirements.txt
RUN /usr/bin/python3 -m venv /app/env
RUN /app/env/bin/pip install -r /app/requirements.txt

# Stage 4 - Copy priors code
FROM stage2 as stage3
RUN /usr/bin/Rscript -e 'devtools::install_github("nikki-t/geoBAMr", force = TRUE)'

# Stage 5 - Download tidyhydat database
FROM stage3 as stage4
RUN sudo mkdir -p /root/.local/share/tidyhydat/\
	&& /usr/bin/Rscript -e 'library(tidyhydat)'\
	&& /usr/bin/Rscript -e 'tidyhydat::download_hydat(ask=FALSE)'
COPY metadata/ /app/metadata/
COPY priors/ /app/priors/
COPY update_priors.py /app/update_priors.py

# Stage 6 - Execute algorithm
FROM stage4 as stage5
LABEL version="1.0" \
	description="Containerized priors module." \
	"confluence.contact"="ntebaldi@umass.edu" \
	"algorithm.contact"="ntebaldi@umass.edu"
ENTRYPOINT ["/app/env/bin/python3", "/app/update_priors.py"]