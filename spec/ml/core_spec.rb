# frozen_string_literal: true

require_relative "support"

# Unit specs for the ML framework itself: params, linalg values, literal
# encoding, the command plumbing, models and summaries, pipelines and tuning.
# Operator-specific specs live in the family files.
RSpec.describe "Spark ML framework" do
  include MlSpecSupport

  let(:ml) { SparkConnect::ML }
  let(:lr_class) { ml::Classification::LogisticRegression }

  describe "params" do
    it "reports the Spark default and the camelCase Spark name" do
      lr = lr_class.new
      expect(lr.max_iter).to eq(100)
      expect(lr.param(:elastic_net_param).spark_name).to eq("elasticNetParam")
      expect(lr.is_set?(:max_iter)).to be(false)
      expect(lr.is_defined?(:max_iter)).to be(true)
    end

    it "sets values with typed validation and chainable setters" do
      lr = lr_class.new
      expect(lr.set_max_iter(5)).to be(lr)
      expect(lr.max_iter).to eq(5)
      expect { lr.set_max_iter("5") }.to raise_error(SparkConnect::IllegalArgumentError, /Integer/)
      expect { lr.set_fit_intercept(1) }.to raise_error(SparkConnect::IllegalArgumentError, /true or false/)
      expect { lr.set(:no_such_param, 1) }.to raise_error(SparkConnect::IllegalArgumentError, /does not have a param/)
    end

    it "coerces a Float double param and keeps Integers for int params" do
      lr = lr_class.new(reg_param: 1)
      expect(lr.reg_param).to be_a(Float)
    end

    it "copies with the same uid and extra values applied" do
      lr = lr_class.new(max_iter: 5)
      copy = lr.copy(reg_param: 0.5)
      expect(copy.uid).to eq(lr.uid)
      expect(copy.reg_param).to eq(0.5)
      expect(lr.reg_param).to eq(0.0)
    end

    it "extracts the full param map and explains params" do
      lr = lr_class.new(max_iter: 7)
      map = lr.extract_param_map
      expect(map[lr.param(:max_iter)]).to eq(7)
      expect(map[lr.param(:tol)]).to eq(1e-6)
      expect(lr.explain_params).to include("maxIter").or include("max_iter")
    end

    it "sends only explicitly set params to the server" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      lr = lr_class.new(max_iter: 3)
      lr.fit(ml_frame(session))
      fit = first_ml(client, :fit).fit
      expect(fit.params.params.keys).to eq(["maxIter"])
      expect(fit.estimator.name).to eq("org.apache.spark.ml.classification.LogisticRegression")
      expect(fit.estimator.type).to eq(:OPERATOR_TYPE_ESTIMATOR)
    end
  end

  describe "linalg" do
    it "builds dense and sparse vectors with Spark's semantics" do
      v = ml::Vectors.dense(1, 2, 3)
      expect(v.to_a).to eq([1.0, 2.0, 3.0])
      expect(v.to_s).to eq("[1.0,2.0,3.0]")
      s = ml::Vectors.sparse(4, [3, 1], [9.0, 5.0])
      expect(s.indices).to eq([1, 3])
      expect(s[3]).to eq(9.0)
      expect(s[0]).to eq(0.0)
      expect(s.to_a).to eq([0.0, 5.0, 0.0, 9.0])
      expect(ml::Vectors.sparse(3, { 0 => 1.0 })).to eq(ml::Vectors.sparse(3, [0], [1.0]))
    end

    it "rejects out-of-range sparse indices" do
      expect { ml::Vectors.sparse(2, [5], [1.0]) }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "builds matrices in column-major order" do
      m = ml::Matrices.dense(2, 2, [1, 2, 3, 4])
      expect(m[0, 1]).to eq(3.0)
      expect(m.to_a).to eq([[1.0, 3.0], [2.0, 4.0]])
      expect(ml::Matrices.dense(2, 2, [1, 2, 3, 4], is_transposed: true)[0, 1]).to eq(2.0)
    end

    it "encodes vectors as VectorUDT structs and decodes them back" do
      v = ml::Vectors.sparse(5, [0, 4], [1.5, -2.0])
      literal = ml::Serialize.literal(v)
      expect(literal.struct.struct_type.udt.jvm_class).to eq("org.apache.spark.ml.linalg.VectorUDT")
      expect(literal.struct.elements.map(&:literal_type)).to eq(%i[byte integer specialized_array specialized_array])
      expect(ml::Serialize.decode(literal)).to eq(v)
    end

    it "round-trips a DenseMatrix literal" do
      m = ml::Matrices.dense(2, 1, [0.5, 1.5])
      decoded = ml::Serialize.decode(ml::Serialize.literal(m))
      expect(decoded).to be_a(ml::Linalg::DenseMatrix)
      expect(decoded.to_a).to eq([[0.5], [1.5]])
    end

    it "converts VectorUDT rows decoded from Arrow structs" do
      udt = SparkConnect::Types::UserDefinedType.new(nil, jvm_class: ml::Linalg::VECTOR_UDT)
      expect(ml::Linalg.convert_udt({ "type" => 1, "size" => nil, "indices" => nil, "values" => [1.0, 2.0] }, udt).to_a).to eq([1.0, 2.0])
      expect(ml::Linalg.convert_udt({ "type" => 0, "size" => 3, "indices" => [2], "values" => [7.0] }, udt)).to be_a(ml::Linalg::SparseVector)
    end
  end

  describe "literal encoding" do
    it "encodes numbers, booleans, strings and arrays" do
      expect(ml::Serialize.literal(3).literal_type).to eq(:integer)
      expect(ml::Serialize.literal(2**40).literal_type).to eq(:long)
      expect(ml::Serialize.literal(0.5).literal_type).to eq(:double)
      expect(ml::Serialize.literal(true).literal_type).to eq(:boolean)
      expect(ml::Serialize.literal("x").literal_type).to eq(:string)
      expect(ml::Serialize.literal([1, 2]).literal_type).to eq(:array)
      expect(ml::Serialize.literal(%w[a b]).literal_type).to eq(:array)
      expect(ml::Serialize.decode(ml::Serialize.literal(%w[a b]))).to eq(%w[a b])
    end
  end

  describe "models and summaries" do
    it "fits through a Fit command and wraps the returned object id" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = lr_class.new.fit(ml_frame(session))
      expect(model).to be_a(ml::Classification::LogisticRegressionModel)
      expect(model.ref_id).to eq("model-1")
      expect(model.uid).to start_with("LogisticRegression_")
    end

    it "fetches an attribute with a Fetch command" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = lr_class.new.fit(ml_frame(session))
      client.responder = ->(_) { SparkConnect::Proto::MlCommandResult.new(param: ml::Serialize.literal(1.25)) }
      expect(model.intercept).to eq(1.25)
      fetch = first_ml(client, :fetch).fetch
      expect(fetch.obj_ref.id).to eq("model-1")
      expect(fetch["methods"].map { |m| m["method"] }).to eq(["intercept"])
    end

    it "builds a summary relation for DataFrame attributes on the training summary" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = lr_class.new.fit(ml_frame(session))
      predictions = model.summary.predictions
      expect(predictions).to be_a(SparkConnect::DataFrame)
      ml_rel = predictions.relation.ml_relation
      expect(ml_rel.fetch["methods"].map { |m| m["method"] }).to eq(%w[summary predictions])
      expect(ml_rel.fetch.obj_ref.id).to eq("model-1")
    end

    it "raises when a model has no training summary" do
      model = lr_class.new.fit(ml_frame(ml_session(MlSpecSupport::FakeMlClient.new)))
      expect do
        ml::Classification::LogisticRegressionModel.new.summary
      end.to raise_error(SparkConnect::IllegalArgumentError, /No training summary/)
      expect(model.has_summary?).to be(true)
    end

    it "transforms through a TransformerRelation over the model object" do
      session = ml_session
      model = lr_class.new.fit(ml_frame(session))
      df = model.transform(ml_frame(session))
      transform = df.relation.ml_relation.transform
      expect(transform.obj_ref.id).to eq("model-1")
      expect(transform.input.range.end).to eq(4)
    end

    it "queues a Delete for a released model and sends it on the next ML call" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = lr_class.new.fit(ml_frame(session))
      model.release
      expect(client.ml_commands.none? { |m| m.command == :delete }).to be(true)
      ml::Rpc.flush_pending(session)
      expect(client.last_ml_command.delete.obj_refs.map(&:id)).to eq([model.ref_id])
    end

    it "keeps a shared model alive until its last copy is released" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      model = lr_class.new.fit(ml_frame(session))
      copy = model.copy
      model.release
      ml::Rpc.flush_pending(session)
      expect(client.ml_commands.any? { |m| m.command == :delete }).to be(false)
      copy.release
      ml::Rpc.flush_pending(session)
      expect(client.last_ml_command.delete.obj_refs.map(&:id)).to eq([model.ref_id])
    end
  end

  describe "server version gating" do
    it "refuses ML commands on Spark 3.5" do
      client = MlSpecSupport::FakeMlClient.new(spark_version: "3.5.5")
      session = ml_session(client)
      expect { ml::Rpc.run(session, SparkConnect::Proto::MlCommand.new) }
        .to raise_error(ml::UnsupportedServerError, /Spark ML over Spark Connect needs .* 4\.0\.0, but the server is 3\.5\.5/)
      expect(client.ml_commands).to be_empty
    end

    it "refuses cache admin commands on Spark 4.0" do
      client = MlSpecSupport::FakeMlClient.new(spark_version: "4.0.0")
      session = ml_session(client)
      expect { ml::Rpc.clean_cache(session) }.to raise_error(ml::UnsupportedServerError, /4\.1\.0/)
    end

    it "allows cache admin commands on Spark 4.1 and later" do
      client = MlSpecSupport::FakeMlClient.new(spark_version: "4.1.0")
      session = ml_session(client)
      expect(ml::Rpc.supports?(session, :cache_admin)).to be(true)
      expect(ml::Rpc.supports?(ml_session(MlSpecSupport::FakeMlClient.new(spark_version: "4.0.2")), :cache_admin)).to be(false)
    end
  end

  describe "evaluators" do
    it "evaluates through an Evaluate command and returns the metric" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      evaluator_class = Class.new(ml::Evaluator) do
        operator "org.apache.spark.ml.evaluation.RegressionEvaluator"
        param :metric_name, :string, default: "rmse", spark_name: "metricName"
      end
      evaluator = evaluator_class.new(metric_name: "r2")
      expect(evaluator.evaluate(ml_frame(session))).to eq(0.5)
      cmd = first_ml(client, :evaluate).evaluate
      expect(cmd.evaluator.name).to eq("org.apache.spark.ml.evaluation.RegressionEvaluator")
      expect(cmd.params.params.keys).to eq(["metricName"])
    end
  end

  describe "pipelines" do
    it "fits each estimator stage on the output of the stages before it" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      scaler_like = ml::Classification::LogisticRegression.new
      pipeline = ml::Pipeline.new(stages: [scaler_like, ml::Classification::LogisticRegression.new])
      model = pipeline.fit(ml_frame(session))
      expect(model).to be_a(ml::PipelineModel)
      expect(model.stages_list.size).to eq(2)
      expect(client.ml_commands.count { |m| m.command == :fit }).to eq(2)
    end

    it "rejects a stage that is neither an estimator nor a transformer" do
      pipeline = ml::Pipeline.new(stages: [Object.new])
      expect { pipeline.fit(ml_frame(ml_session)) }.to raise_error(SparkConnect::IllegalArgumentError, /pipeline stage/)
    end
  end

  describe "tuning" do
    let(:evaluator) do
      Class.new(ml::Evaluator) do
        operator "org.apache.spark.ml.evaluation.RegressionEvaluator"
        param :metric_name, :string, default: "rmse", spark_name: "metricName"
      end.new
    end

    it "builds the cartesian product of a param grid" do
      lr = lr_class.new
      grid = ml::ParamGridBuilder.new.add_grid(lr.param(:reg_param), [0.1, 0.2]).add_grid(lr.param(:max_iter), [3, 4, 5]).build
      expect(grid.size).to eq(6)
      expect(grid.first.keys.map(&:name)).to eq(%i[reg_param max_iter])
    end

    it "runs cross validation: folds x maps fits plus one refit, and keeps the best map" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      lr = lr_class.new
      grid = ml::ParamGridBuilder.new.add_grid(lr.param(:reg_param), [0.1, 0.2]).build
      cv = ml::CrossValidator.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator, num_folds: 2, seed: 42)
      model = cv.fit(ml_frame(session, 10))
      expect(model).to be_a(ml::CrossValidatorModel)
      expect(model.avg_metrics).to eq([0.5, 0.5])
      expect(client.ml_commands.count { |m| m.command == :fit }).to eq((2 * 2) + 1)
      expect(model.transform(ml_frame(session))).to be_a(SparkConnect::DataFrame)
    end

    it "rejects fewer than two folds" do
      expect { ml::CrossValidator.new(estimator: lr_class.new, estimator_param_maps: [], evaluator: evaluator, num_folds: 1) }
        .to raise_error(SparkConnect::IllegalArgumentError, /at least 2/)
    end

    it "runs a train-validation split with one fit per map plus the refit" do
      client = MlSpecSupport::FakeMlClient.new
      session = ml_session(client)
      lr = lr_class.new
      grid = ml::ParamGridBuilder.new.add_grid(lr.param(:max_iter), [3, 4]).build
      tvs = ml::TrainValidationSplit.new(estimator: lr, estimator_param_maps: grid, evaluator: evaluator, seed: 1)
      model = tvs.fit(ml_frame(session, 10))
      expect(model.validation_metrics).to eq([0.5, 0.5])
      expect(client.ml_commands.count { |m| m.command == :fit }).to eq(3)
      expect(tvs.train_ratio).to eq(0.75)
    end
  end

  describe "stat and functions" do
    it "builds a correlation relation over the helper object" do
      session = ml_session
      df = ml::Stat::Correlation.corr(ml_frame(session), "features", "spearman")
      fetch = df.relation.ml_relation.fetch
      expect(fetch.obj_ref.id).to eq(ml::Rpc::HELPER_ID)
      expect(fetch["methods"].first["method"]).to eq("correlation")
    end

    it "builds the internal aggregate_metrics call for Summarizer" do
      col = ml::Stat::Summarizer.metrics("mean").summary(SparkConnect::Column.from_name("features"))
      expect(col.to_expr.unresolved_function.function_name).to eq("aggregate_metrics")
      expect(col.to_expr.unresolved_function.is_internal).to be(true)
      expect(ml::Stat::Summarizer.mean(SparkConnect::Column.from_name("features")).to_expr.unresolved_extract_value).to be_truthy
    end

    it "rejects unknown Summarizer metrics" do
      expect { ml::Stat::Summarizer.metrics("bogus") }.to raise_error(SparkConnect::IllegalArgumentError)
    end

    it "builds vector_to_array and array_to_vector as internal calls" do
      expect(ml::Functions.vector_to_array("v").to_expr.unresolved_function.function_name).to eq("vector_to_array")
      expect(ml::Functions.array_to_vector("a").to_expr.unresolved_function.is_internal).to be(true)
    end
  end
end
