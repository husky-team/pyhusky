#include "job_handler.hpp"

#include <core/engine.hpp>
#include <core/zmq_helpers.hpp>
#include <lib/graphbase.hpp>
#include <lib/pagerank.hpp>
#include <lib/inputformat/lineinputformat.hpp>
#include <lib/topk.hpp>
#include <task.hpp>
#include <opdag.hpp>
#include <post.hpp>
#include <python_threadhandler.hpp>
#include <python_pythonhandler.hpp>
#include <scalautils.hpp>

namespace husky {

std::unordered_map<std::string, std::function<bool(JobInfo&)>> JobHandler::job_handler_map;
std::unordered_map<std::string, std::function<void(DaemonInfo&)>> DaemonHandler::daemon_handler_map;

void JobHandler::job_listener(JobInfo & job_info) {
    while(true) {
        job_info.instr = job_info.to_daemon.recv_string();
        debug_msg("job_listener: "+job_info.instr);
        if (JobHandler::job_handler_map.find(job_info.instr) != JobHandler::job_handler_map.end()) {
            bool exit = JobHandler::job_handler_map[job_info.instr](job_info);
            if (exit) return;
        } else {
            throw std::runtime_error("Weird message received in job_listener");
        }
    }
}
void JobHandler::init_job_handler_map() {
    job_handler_map["session_begin_py"] = session_begin_py;
    job_handler_map["session_end_py"] = session_end_py;
    job_handler_map["session_begin_sc"] = session_begin_sc;
    job_handler_map["session_end_sc"] = session_end_sc;
    job_handler_map["new_instr_sc"] = new_instr_sc;
    job_handler_map["new_instr_py"] = new_instr_py;
}

bool JobHandler::session_begin_py(JobInfo& job_info) {
    // get worker
    Context::init_local();
    Context::get_local_tid() = job_info.local_worker_id;
    Context::get_global_tid() = job_info.global_worker_id;
    // Context::get_worker<AggregatorWorker>();
    Context::get_worker<BaseWorker>();
    job_info.py_handler = new PythonHandler(job_info.to_daemon, job_info.local_worker_id);
    job_info.py_handler->start_python_proc(job_info.local_worker_id);
    return 0;
}
bool JobHandler::session_end_py(JobInfo& job_info) {
    // free worker
    job_info.py_handler->send_string(job_info.instr);
    job_info.py_handler->close_python_proc();
    delete job_info.py_handler;
    Context::free_worker();
    Context::finalize_local();
    debug_msg("closed");
    return 1;
}
bool JobHandler::session_begin_sc(JobInfo& job_info) {
    Context::init_local();
    Context::get_local_tid() = job_info.local_worker_id;
    Context::get_global_tid() = job_info.global_worker_id;
    //mind here
    Context::get_worker<ScalaWorker>().setUtils(PostOffice::getOffice().getPostbox(job_info.local_worker_id + 1), job_info.to_daemon); 
    debug_msg("sc_begin end");
    return 0;
}
bool JobHandler::session_end_sc(JobInfo& job_info) {
    Context::free_worker();
    Context::finalize_local();
    debug_msg("session sc closed");
    return 1;
}
bool JobHandler::new_instr_sc(JobInfo& job_info) {
    debug_msg("instr sc starts");
    BinStream job_stream = job_info.to_daemon.recv_binstream(); 
    OpDAG opdag;
    job_stream >> opdag;
    auto & leave = opdag.get_leaves()[0];
    auto & worker = Context::get_worker<ScalaWorker>();
    auto & to_scala = PostOffice::getOffice().getPostbox(job_info.local_worker_id + 1);
    const auto & op_name = leave->get_op().get_name();
    if (op_name == "load_sc") {
        Husky::LineInputFormat<Husky::HDFSFileSplitter> infmt;
        debug_msg(leave->get_op().get_param("Path"));
        infmt.set_input(leave->get_op().get_param("Path"));
        Context::get_worker<BaseWorker>().load(infmt, [&](boost::string_ref & chunk) {
            to_scala.send(std::string(&chunk[0], chunk.size()));
            if (to_scala.hasMailToRead())
                recv_from_scala(worker);
        });
        debug_msg("finish_load");
    } else if (op_name == "reduceByKey_end_sc" || op_name == "groupByKey_end_sc") {
        const auto & list_name = leave->get_op().get_param("listName");
        debug_msg("list execution on " + list_name);
        auto & worker = Context::get_worker<ScalaWorker>();
        auto & obj_list = Context::get_objlist<ScalaObject>(list_name, false);
        worker.registerScalaObjectCreator(obj_list);
        worker.list_execute(obj_list, [&](ScalaObject & obj) {
            to_scala.send(obj.id());
            auto & msg = get_messages<BinStream>(obj);
            ASSERT_MSG(msg.size() < 0x7fffffff, "If you see this, then contact zzxx to change int to long long");
            to_scala.send(int(msg.size()));
            for (auto & i : msg)
                to_scala.send(i);
            if (to_scala.hasMailToRead())
                recv_from_scala(worker); 
        });
        debug_msg("finish " + op_name);
    } else if (op_name == "reduce_end_sc") {
        auto & worker = Context::get_worker<ScalaWorker>();
        worker.processReduceListMessage();
        //scope_list provides an environment where I can send message, write_to_hdfs .. etc
        worker.list_execute(worker.scope_list, [&](ScalaObject & scobj) {
            auto obj = worker.findReduceObject(leave->get_op().get_param("listName"));
            //only one worker may find the object, or the object doesn't exist
            if (obj) {
                auto & msg = get_messages<BinStream>(*obj);
                ASSERT_MSG(msg.size() < 0x7fffffff, "If you see this, then contact zzxx to change int to long long");
                to_scala.send(int(msg.size()));
                for (auto & i : msg)
                    to_scala.send(i);
                debug_msg("finish reduce_end");
            } else {
                to_scala.send(0);//for those who is not the owner of the object
            }
        });
    }
    debug_msg("job_listener: end instr sc");
    return 0;
}
bool JobHandler::new_instr_py(JobInfo& job_info) {
    debug_msg("job_listener: start instr");
    // zmq_send_string(job_util.pipe_to_python, instr);
    job_info.py_handler->send_string(job_info.instr);
    // BinStream job_stream = zmq_recv_binstream(job_util.thread_instr_recver);
    BinStream job_stream = job_info.to_daemon.recv_binstream();
    // zmq_send_binstream(job_util.pipe_to_python, job_stream);
    job_info.py_handler->send_binstream(job_stream);
    OpDAG opdag;
    job_stream >> opdag;
    ASSERT_MSG(opdag.get_leaves().size() == 1, "not single input");
    auto leave = opdag.get_leaves()[0];
    if (leave->get_op().get_name() == "load_py") {
        Husky::LineInputFormat<Husky::HDFSFileSplitter> infmt;
        std::string url = leave->get_op().get_param(0);
        infmt.set_input(url);
        Context::get_worker<BaseWorker>().load(infmt, [&](boost::string_ref & chunk) {
            job_info.py_handler->send_string(std::string(&chunk[0], chunk.size()));
            // zmq_send_string(job_util.pipe_to_python, std::string(&chunk[0], chunk.size()));
            // debug_msg("read: "+ chunk.to_string());
        });
        // zmq_send_string(job_util.pipe_to_python, "");
        job_info.py_handler->send_string("");
    }
    // visit_dag(opdag);
    // listen_from_python(job_util);
    job_info.py_handler->listen_from_python();
    debug_msg("job_listener: end instr");
    return 0;
}



/// DaemonHandler

void DaemonHandler::daemon_run(const std::string & config_file) {
    Context::init_global(NULL, config_file);
    gen_worker_info(Context::get_params("socket_file"), Context::get_worker_info());
    DaemonInfo daemon_info(config_file);
    while(true) {
        BinStream bin_proc_id;
        bin_proc_id << Context::get_worker_info().get_proc_id();
        daemon_info.dag_bin = Context::get_coordinator().ask_master(bin_proc_id, TYPE_REQ_INSTR);
        if(daemon_info.dag_bin.size() == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        daemon_info.dag_bin >> daemon_info.instr;
        debug_msg("received job: " + daemon_info.instr);

        if (DaemonHandler::daemon_handler_map.find(daemon_info.instr) != DaemonHandler::daemon_handler_map.end()) {
            DaemonHandler::daemon_handler_map[daemon_info.instr](daemon_info);
        } else {
            throw std::runtime_error("Weird message received in daemon");
        }
    }
}

void DaemonHandler::init_daemon_handler_map() {
    daemon_handler_map["session_begin_py"] = session_begin;
    daemon_handler_map["session_begin_sc"] = session_begin;
    daemon_handler_map["session_end_py"] = session_end;
    daemon_handler_map["session_end_sc"] = session_end;
    daemon_handler_map["new_instr_py"] = new_instr_py;
    daemon_handler_map["new_instr_sc"] = new_instr_sc;
}
void DaemonHandler::session_begin(DaemonInfo& daemon_info) {
    read_config(daemon_info.config_file, Context::get_params());
    int proc_id = Context::get_worker_info().get_proc_id();
    if(proc_id == -1) {
        throw std::runtime_error("proc_id error");
    }

    start_mailbox();
    Context::get_coordinator().serve();

    //sc
    if (daemon_info.instr == "session_begin_sc") {
        PostOffice::newOffice().serve();
        if (Context::get_params("scala.backend.type") != "remote") {
            const char* cmd = "scala -Djna.library.path=./schusky/zmq/lib ./schusky/schusky-backend.jar";
            auto pid = fork();
            if (pid < 0) {//original process, failed to create a child process
                throw std::runtime_error("Failed to open scala backend");
            }
            if (pid == 0) {//child process
                execl("/bin/sh", "sh", "-c", cmd);
                //Exception will be thrown only when execl is executed unsuccessfully.
                throw std::runtime_error("Failed to open scala backend");
            }
        }
    }
    //sc

    int local_worker_id = 0;
    for(int i=0; i<Context::get_worker_info().get_num_workers(); i++) {
        if(Context::get_worker_info().get_proc_id(i) != proc_id) continue;
        auto & to_daemon = daemon_info.th_handler.newITCWorker(local_worker_id);
        daemon_info.threads.push_back(new std::thread( [=,&to_daemon](){
            JobInfo job_info(i, local_worker_id, to_daemon);
            JobHandler::job_listener(job_info);
        }));
        ++local_worker_id;
    }

    daemon_info.th_handler.broadcast_to_threads(daemon_info.instr);

    BinStream bin_proc_id;
    bin_proc_id << Context::get_worker_info().get_proc_id();
    Context::get_coordinator().ask_master(bin_proc_id, TYPE_TASK_END);
}

void DaemonHandler::session_end(DaemonInfo& daemon_info) {
    //sc
    if (daemon_info.instr == "session_end_sc") {
        PostOffice::closeOffice();
        debug_msg("Post Office Closed");
    }
    //sc

    daemon_info.th_handler.broadcast_to_threads(daemon_info.instr);

    for(int i=0; i<daemon_info.threads.size(); i++) {
        daemon_info.threads[i]->join();
        delete daemon_info.threads[i];
    }
    daemon_info.threads.clear();

    Context::finalize_global();

    Context::init_global(NULL, daemon_info.config_file);
    gen_worker_info(Context::get_params("socket_file"), Context::get_worker_info());
    
    BinStream bin_proc_id;
    bin_proc_id << Context::get_worker_info().get_proc_id();
    Context::get_coordinator().ask_master(bin_proc_id, TYPE_TASK_END);
}
void DaemonHandler::new_instr_py(DaemonInfo& daemon_info) {
    daemon_info.th_handler.broadcast_to_threads(daemon_info.instr);
    daemon_info.th_handler.broadcast_to_threads(daemon_info.dag_bin);
    // wait for workers ...
    // tell master
    BinStream result_buf;
    result_buf << Context::get_worker_info().get_proc_id();

    daemon_info.th_handler.listen_from_threads(result_buf);

    Context::get_coordinator().ask_master(result_buf, TYPE_TASK_END);
}
void DaemonHandler::new_instr_sc(DaemonInfo& daemon_info) {
    auto & to_scala = PostOffice::getOffice().getPostbox(0);
    std::string root_name;
    daemon_info.dag_bin >> root_name;
    debug_msg("Root Name: " + root_name);
    to_scala.send(root_name);
    to_scala.send(daemon_info.dag_bin);
    if (root_name != "jar_sc") {
        daemon_info.th_handler.broadcast_to_threads(daemon_info.instr);
        daemon_info.th_handler.broadcast_to_threads(daemon_info.dag_bin);
        daemon_info.th_handler.listen_from_threads_sc();
    }
    BinStream proc_id_bin;
    proc_id_bin << Context::get_worker_info().get_proc_id();
    Context::get_coordinator().ask_master(proc_id_bin, TYPE_TASK_END);
}

}; // namespace husky
