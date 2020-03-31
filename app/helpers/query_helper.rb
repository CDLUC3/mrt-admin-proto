module QueryHelper

  def arkLink(ark)
    ark = URI.escape(ark, Regexp.new("[^#{URI::PATTERN::UNRESERVED}]"))
    "#{ENV['MERRITT_URL']}/m/#{ark}"
  end
end
