# -*- coding: UTF-8 -*-
"""
This module provides multiprocessing Runner class.

Core functionality was taken from: https://github.com/xrg/behave-parallel/tree/eparallel
"""

import six
import multiprocessing

from behave.formatter._registry import make_formatters
from behave.runner import Runner, Context
from behave.model import Feature, Scenario, ScenarioOutline, NoMatch
from behave.runner_util import parse_features
from behave.step_registry import registry as the_step_registry

if six.PY2:
    import Queue as queue
else:
    import queue


class MasterParallelRunner(Runner):
    """Master parallel runner: scans jobs and distributes to slaves
        This runner should not do any "processing" tasks, apart from scanning
        the feature files and their scenarios. It then spawns processing nodes
        and lets them consume the queue of tasks scheduled.
    """
    def __init__(self, config):
        super(MasterParallelRunner, self).__init__(config)
        self.jobs_map = {}
        self.jobsq = multiprocessing.JoinableQueue()
        self.resultsq = multiprocessing.Queue()
        self._reported_features = set()
        self.results_fail = False

    def run_with_paths(self):
        feature_locations = [
            filename for filename in self.feature_locations() if not self.config.exclude(filename)
            ]
        # hooks themselves not used, but 'environment.py' loaded
        self.load_hooks()
        # step definitions are needed here for formatters only
        self.load_step_definitions()

        # get list of all available features
        features = parse_features(feature_locations, language=self.config.lang)

        # leave only features/scenarios which should be run
        [self.features.append(feature) for feature in features if feature.should_run(self.config)]

        # get feature/scenario count for multiprocessing
        feature_count, scenario_count = self.scan_features()
        njobs = len(self.jobs_map)
        proc_count = int(self.config.proc_count)

        print("\n\nINFO: {0} scenario(s) and {1} feature(s) queued for"
                " consideration by {2} workers.\n\n"
               .format(scenario_count, feature_count, proc_count))
        processes = []

        # -- STEP: Prepare formatters to write messages to the default Stream (Master Process)
        stream_openers = self.config.outputs
        self.formatters = make_formatters(self.config, stream_openers)

        # -- STEP: init default context for Master Process to execute before/after_all() hooks
        self.context = Context(self)
        self.setup_capture()
        self.run_hook("before_all", self.context)

        # -- STEP: Run each test as a separate Process
        for i in range(proc_count):
            client = ProcessClientExecutor(self, i)
            p = multiprocessing.Process(
                target=client.run_executor,
                args=[self.context]
                )
            processes.append(p)
            p.start()
            del p

        print("INFO: started {0} workers for {1} jobs.".format(proc_count, njobs))

        while (not self.jobsq.empty()):
            # 1: consume results while tests are running
            self.consume_results()
            if not any([p.is_alive() for p in processes]):
                break

        # wait for all jobs to be processed
        if any([p.is_alive() for p in processes]):
            self.jobsq.join()
            print("INFO: all jobs have been processed")

            while self.consume_results(timeout=0.1):
                # 2: remaining results
                pass

            # then, wait for all workers to exit:
            [p.join() for p in processes]
        print("INFO: all sub-processes have returned")

        # -- STEP: Run after_all() hook
        self.run_hook("after_all", self.context)

        while self.consume_results(timeout=0.1):
            # 3: just in case some arrive late in the pipe
            pass

        for f in self.features:
            # make sure all features (including ones that have not returned) are printed
            self._output_feature(f)

        # notify formatters and reporters that test run has finished
        for formatter in self.formatters:
            formatter.close()
        for reporter in self.config.reporters:
            reporter.end()

        return self.results_fail

    def scan_features(self):
        raise NotImplementedError

    def put_item_into_queue(self, item: Scenario | Feature):
        item_id = id(item)
        self.jobs_map[item_id] = item
        self.jobsq.put(item_id)

    def consume_results(self, timeout=1):
        """Get item result in real-time using multiprocessing pipe. 
        
        Required for formatters to work correctly
        """

        try:
            job_id, result = self.resultsq.get(timeout=timeout)
        except queue.Empty:
            return False

        if job_id is None and result == 'set_fail':
            self.results_fail = True
            return True

        item = self.jobs_map.get(job_id)
        if item is None:
            print("ERROR: job_id=%x not found in master map" % job_id)
            return True

        try:
            item.recv_status(result)
            if isinstance(item, Feature):
                self._output_feature(item)
            elif isinstance(item, Scenario):
                feature = item.feature
                print("INFO: scenario finished: %s %s" % (item.name, item.status))
                if feature.is_finished:
                    self._output_feature(feature)
        except Exception as e:
            print("ERROR: cannot receive status for %r: %s" % (item, e))
            if self.config.wip and not self.config.quiet:
                import traceback
                traceback.print_exc()
        return True

    def _output_feature(self, feature):
        if id(feature) in self._reported_features:
            return
        self._reported_features.add(id(feature))

        def _out_scenario(scenario, formatter):
            formatter.scenario(scenario)
            for step in scenario.steps:
                formatter.step(step)
            for step in scenario.steps:
                match = the_step_registry.find_match(step)
                if match:
                    formatter.match(match)
                else:
                    formatter.match(NoMatch())
                formatter.result(step)

        for formatter in self.formatters:
            formatter.uri(feature.filename)
            formatter.feature(feature)
            if feature.background:
                formatter.background(feature.background)
            for scenario in feature.scenarios:
                if isinstance(scenario, ScenarioOutline):
                    for scen in scenario.scenarios:
                        _out_scenario(scen, formatter)
                else:
                    _out_scenario(scenario, formatter)
            # notify formatter the end of current test
            formatter.eof()

        for reporter in self.config.reporters:
            reporter.feature(feature)


class FeatureParallelRunner(MasterParallelRunner):
    """Adds features to job queue"""

    def scan_features(self):
        n_features = 0

        for feature in self.features:
            self.put_item_into_queue(feature)
            n_features += 1

            # compute background steps for Scenarios and SubScenarios
            for scenario in feature.scenarios:
                scenario.background_steps
                if isinstance(scenario, ScenarioOutline):
                    for sub_scenario in scenario.scenarios:
                        sub_scenario.background_steps

        return n_features, 0


class ScenarioParallelRunner(MasterParallelRunner):
    """Adds each scenario/sub-scenario to job queue"""

    def scan_features(self):
        n_features = n_scenarios = 0

        for feature in self.features:
            # compute background steps for Scenarios and SubScenarios
            for scenario in feature.scenarios:
                scenario.background_steps
                if scenario.type == 'scenario':
                    self.put_item_into_queue(scenario)
                    n_scenarios += 1
                else:
                    for sub_scenario in scenario.scenarios:
                        sub_scenario.background_steps
                        self.put_item_into_queue(sub_scenario)
                        n_scenarios += 1

        return n_features, n_scenarios


class ProcessClientExecutor(Runner):
    """Multiprocessing Client Executor: picks "job" from parent queue and runs it
        Each client is tagged with a `num` to appear in outputs etc.
    """
    def __init__(self, parent, num):
        super(ProcessClientExecutor, self).__init__(parent.config)
        self.num = num
        self.jobs_map = parent.jobs_map
        self.jobsq = parent.jobsq
        self.resultsq = parent.resultsq

    def iter_queue(self):
        """Iterator fetching features from the queue
            Note that this iterator is lazy and multiprocess-affected:
            it cannot know its set of features in advance, will dynamically
            yield ones as found in the queue
        """
        while True:
            try:
                job_id = self.jobsq.get(timeout=1.0)
            except queue.Empty:
                break

            job = self.jobs_map.get(job_id, None)
            if job is None:
                print("ERROR: missing job id=%s from map" % job_id)
                self.jobsq.task_done()
                continue

            if isinstance(job, Feature):
                yield job
            elif isinstance(job, Scenario):
                # construct a dummy feature, having only this scenario
                kwargs = {}
                for k in ('filename', 'line', 'keyword', 'name', 'tags',
                          'description', 'background', 'language'):
                    kwargs[k] = getattr(job.feature, k)
                kwargs['scenarios'] = [job]
                orig_parser = job.feature.parser
                feature = Feature(**kwargs)
                feature.parser = orig_parser
                yield feature
            else:
                raise TypeError("Don't know how to process: %s" % type(job))

            try:
                self.resultsq.put((job_id, job.send_status()))
            except Exception as e:
                print("ERROR: Cannot send result for {1}: {0}".format(e, job.name))

            self.jobsq.task_done()

    def update_executors_context(self, master_context: Context):
        """Loads values from master context into the local context
        
        It's needed to have access to context objects, which were initialized in before_all()
        In this case, each ProcessClientExecutor.context will have a reference to the same object
        created in Master Process.
        """
        for k, v in master_context._root.items():
            if k not in self.context._root and k not in ["stdout_capture", "stderr_capture"]:
                try:
                    setattr(self.context, k, v)
                    self.context._origin[k] = master_context._origin[k]
                except BaseException as e:
                    print(f"ERROR: Failed to updated context with {k} --> {v}: ", e)

    def run_executor(self, master_context: Context):
        with self.path_manager:
            self.setup_paths()
            return self.run_with_paths(master_context)

    def run_with_paths(self, master_context: Context):
        self.context = Context(self)
        self.update_executors_context(master_context)

        self.load_hooks()
        self.load_step_definitions()
        assert not self.aborted

        failed = self.run_model(features=self.iter_queue())
        if failed:
            self.resultsq.put((None, 'set_fail'))
        self.resultsq.close()

    def run_model(self, features=None):
        # pylint: disable=too-many-branches
        if not self.context:
            self.context = Context(self)
        if self.step_registry is None:
            self.step_registry = the_step_registry
        if features is None:
            features = self.features

        # -- ENSURE: context.execute_steps() works in weird cases (hooks, ...)
        self.hook_failures = 0
        self.setup_capture()

        run_feature = not self.aborted
        failed_count = 0
        undefined_steps_initial_size = len(self.undefined_steps)
        for feature in features:
            if run_feature:
                try:
                    self.feature = feature
                    for formatter in self.formatters:
                        formatter.uri(feature.filename)

                    failed = feature.run(self)
                    if failed:
                        failed_count += 1
                        if self.config.stop or self.aborted:
                            # -- FAIL-EARLY: After first failure.
                            run_feature = False
                except KeyboardInterrupt:
                    self.abort(reason="KeyboardInterrupt")
                    failed_count += 1
                    run_feature = False

            # -- ALWAYS: Report run/not-run feature to reporters.
            # REQUIRED-FOR: Summary to keep track of untested features.
            for reporter in self.config.reporters:
                reporter.feature(feature)

        # -- CLEAN-UP:
        # pylint: disable=protected-access, broad-except
        cleanups_failed = False
        try:
            # Without dropping the last context layer
            self.context._do_cleanups()
        except Exception:
            cleanups_failed = True

        if self.aborted:
            print("\nABORTED: By user.")

        failed = ((failed_count > 0) or self.aborted or (self.hook_failures > 0)
                  or (len(self.undefined_steps) > undefined_steps_initial_size)
                  or cleanups_failed)
                  # XXX-MAYBE: or context.failed)
        return failed
