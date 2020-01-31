require 'rails_helper'

RSpec.describe MerrittObject, type: :model do
  it "test model" do
    cat = "meow"
    expect(cat).to eq("meow")
  end
end
