require "json"
require "uuid"
require "./store"

store = EventStore.new

class Login
  property :email, :password, :customer_id

  def initialize(@email : String, @password : String, @customer_id : String)
  end
end

class Customer
  property :id
  def initialize(@id : String)
  end
end

abstract class Svc
  @handlers = Hash(String, Array(EventHandler)).new
  @pos : UInt64
  @stream_conf : { stream: String, category: String? }?

  def initialize(@store : EventStore, @pos = 0_u64)
  end

  def on(event_type : String, &block : EventObj -> Nil)
    return @handlers[event_type] << block if @handlers[event_type]?
    @handlers[event_type] = [block]
  end

  def handle(evt)
    return if @handlers[evt.message_type]?.nil?
    @handlers[evt.message_type].each do |handler|
      handler.call(evt)
    end
  end

  abstract def setup

  def listen_to(stream : String = "*", category = nil)
    @stream_conf = { stream: stream, category: category }
  end

  def run
    setup
    conf = @stream_conf.not_nil!
    loop do
      @pos, events = @store.read(
        conf[:stream],
        category: conf[:category],
        position: @pos
      )
      events.each { |e| handle(e) }
      sleep 0.1
    end
  end
end

class CustomerCreatorSvc < Svc
  def setup
    listen_to("customer-commands")

    on "CreateCustomer", do |evt|
      customer_id = evt.data["id"].as_s
      customer = Customer.new(customer_id)
      stream = Stream.new(@store, "customer", customer_id)
      stream.write("CustomerCreated", {
        "id" => customer.id,
        "email" => "bob@example.com",
        "name" => "Bob",
        "type" => "wholesaler",
        "likes" => "cats"
      })
      nil
    end
  end
end

class AuthSvc < Svc
  def setup
    listen_to category: "customer"

    on "CustomerCreated" do |evt|
      # customer = CustomerDatabase.fetch(evt.data["id"])
      customer_id = evt.data["id"].as_s
      login = Login.new(evt.data["email"].as_s, UUID.random.to_s, customer_id)
      login_stream = Stream.new(@store, "login", customer_id)
      login_stream.write("LoginCreated", { "email" => "bob@example.com" })
      customer_stream = Stream.new(@store, "customer", customer_id)
      customer_stream.write("LoginCreatedForCustomer", {
        "customer_id" => UUID.random.to_s,
        "email" => "bob@example.com",
      })
      nil
    end
  end
end

spawn { CustomerCreatorSvc.new(store).run }
spawn { AuthSvc.new(store).run }

stream = Stream.new(store, "customer", "commands")
stream.write("CreateCustomer", {
  "id" => UUID.random.to_s,
  "email" => "bob@example.com",
  "name" => "Bob",
  "type" => "wholesaler",
  "likes" => "cats"
})

sleep 0.5

store.read[1].each { |e| puts e }
