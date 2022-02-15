FROM spre-base
USER root  
RUN export SAS_LICENSE=
RUN echo $SAS_LICENSE > /tmp/license.sas
RUN /opt/sas/viya/home/SASFoundation/utilities/bin/apply_license_viya4 /tmp/license.sas
WORKDIR /tmp
RUN yum install gcc openssl-devel bzip2-devel libffi-devel zlib-devel -y                                              
RUN curl https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz --output /tmp/Python-3.8.12.tgz                
RUN tar xzf Python-3.8.12.tgz    
WORKDIR /tmp/Python-3.8.12
RUN ./configure --enable-optimizations                 
RUN yum install make -y                
RUN make altinstall                       
RUN yum install which -y                      
WORKDIR /tmp                                               
RUN rm -r Python-3.8.12.tgz
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
WORKDIR /
RUN alternatives --install /usr/bin/python3 python3 /usr/local/bin/python3.8 1
RUN alternatives --set python3 /usr/local/bin/python3.8
RUN alternatives --auto python3
RUN python3.8 -m pip install prefect[github] dask distributed pandas mumpy saspy dask-kubernetes bokeh
RUN ln -s /usr/local/python3 python
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["tini", "-g", "--", "entrypoint.sh"]