Embulk::JavaPlugin.register_input(
  "spanner", "org.embulk.input.spanner.SpannerInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
