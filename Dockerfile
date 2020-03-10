#*********************************************************************
#   Copyright 2019 Regents of the University of California
#   All rights reserved
#*********************************************************************
#   docker build -t cdluc3/dryad .
#
# Create super user example:
# docker exec -it dryad-db mysql --password=root-password --database=dryad
# update stash_engine_users set role='superuser' where orcid='0000-0002-5961-0685';

FROM ruby:2.6.3
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add - \
  && echo 'deb http://dl.yarnpkg.com/debian/ stable main' > /etc/apt/sources.list.d/yarn.list
RUN apt-get update -qq && apt-get install -y build-essential libpq-dev nodejs yarn

RUN gem install bundler:2.1.2

# Set an environment variable where the Rails app is installed to inside of Docker image
ENV RAILS_ROOT /app

# Set working directory
WORKDIR $RAILS_ROOT

# Setting env up
ENV RAILS_ENV='development'
ENV RACK_ENV='development'

COPY Gemfile Gemfile
COPY Gemfile.lock Gemfile.lock

RUN bundle install --jobs 20 --retry 5

COPY . .

RUN bundle exec rake assets:precompile

EXPOSE 3000 9292
CMD ["bundle", "exec", "puma", "-C", "config/application.rb"]
