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
	&& apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 \
	&& . /etc/lsb-release \
	&& echo "deb https://cloud.r-project.org/bin/linux/ubuntu ${DISTRIB_CODENAME}-cran40/" >> /etc/apt/sources.list \
	&& /usr/bin/curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

# Stage 1 - AWS CLI
FROM stage0 as stage1
RUN /usr/bin/unzip awscliv2.zip 
RUN ./aws/install 
RUN /usr/local/bin/aws configure set default.region us-west-2
COPY credentials /root/.aws/credentials

# Stage 2 - R and R packages (geoBAM)
FROM stage1 as stage2
COPY remove_packages.R /app/remove_packages.R
RUN apt update && apt upgrade -y && apt -y install r-base r-base-dev \
	&& rm -rf /var/lib/apt/lists/* \
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
	&&/usr/bin/Rscript -e "install.packages('rvest', dependencies=TRUE, repos='http://cran.rstudio.com/')"\
	&& /usr/bin/Rscript -e 'devtools::install_github("nikki-t/geoBAMr", force = TRUE)'
#Stage 3 - Python packages
FROM stage2 as stage3
COPY requirements.txt /app/requirements.txt
RUN /usr/bin/python3 -m venv /app/env
RUN /app/env/bin/pip install -r /app/requirements.txt

# Stage 4 - Copy priors code
FROM stage3 as stage4
COPY priors/ /app/priors/
COPY update_priors.py /app/update_priors.py

# Stage 5 - Download tidyhydat database
FROM stage4 as stage5
RUN sudo mkdir -p /root/.local/share/tidyhydat/\
	&& /usr/bin/Rscript -e 'library(tidyhydat)'\
	&& /usr/bin/Rscript -e 'tidyhydat::download_hydat(ask=FALSE)'
# Stage 6 - Execute algorithm
FROM stage5 as stage6
LABEL version="1.0" \
	description="Containerized priors module." \
	"confluence.contact"="ntebaldi@umass.edu" \
	"algorithm.contact"="ntebaldi@umass.edu"
ENTRYPOINT ["/app/env/bin/python3", "/app/update_priors.py"]