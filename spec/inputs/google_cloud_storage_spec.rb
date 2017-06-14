# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/google_cloud_storage"

describe LogStash::Inputs::GoogleCloudStorage do

let(:client) { double("google-client") }
  let(:service_account) { double("service-account") }
  let(:key)    { "key" }

  before(:each) do
    allow(Google::APIClient).to receive(:new).and_return(client)
    allow(client).to receive(:discovered_api).with("storage", "v1")
    allow(Google::APIClient::PKCS12).to receive(:load_key).with("", "notasecret").and_return(key)
    allow(Google::APIClient::JWTAsserter).to receive(:new).and_return(service_account)
    allow(client).to receive(:authorization=)
    allow(service_account).to receive(:authorize)
  end


  it "should register without errors" do
    plugin = LogStash::Plugin.lookup("input", "google_cloud_storage").new({"bucket" => "", "key_path" => "", "service_account" => ""})
    expect { plugin.register }.to_not raise_error
  end

end
