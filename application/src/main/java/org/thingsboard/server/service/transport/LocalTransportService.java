/**
 * Copyright © 2016-2019 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.service.transport;

import akka.actor.ActorRef;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.DeferredResult;
import org.thingsboard.rule.engine.api.util.DonAsynchron;
import org.thingsboard.server.actors.ActorSystemContext;
import org.thingsboard.server.common.msg.cluster.ServerAddress;
import org.thingsboard.server.common.transport.TransportServiceCallback;
import org.thingsboard.server.common.transport.service.AbstractTransportService;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.*;
import org.thingsboard.server.service.cluster.routing.ClusterRoutingService;
import org.thingsboard.server.service.cluster.rpc.ClusterRpcService;
import org.thingsboard.server.service.encoding.DataDecodingEncodingService;
import org.thingsboard.server.service.transport.msg.TransportToDeviceActorMsgWrapper;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Created by ashvayka on 12.10.18.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "transport", value = "type", havingValue = "local")
public class LocalTransportService extends AbstractTransportService implements RuleEngineTransportService {

    @Autowired
    private TransportApiService transportApiService;

    @Autowired
    private ActorSystemContext actorContext;

    //TODO: completely replace this routing with the Kafka routing by partition ids.
    @Autowired
    private ClusterRoutingService routingService;
    @Autowired
    private ClusterRpcService rpcService;
    @Autowired
    private DataDecodingEncodingService encodingService;

    // 响应列表
    private HashMap<String, String> m_cResponseMap;
    // 读写锁
    private ReadWriteLock rwlock = new ReentrantReadWriteLock();
    // 定时器
    private Timer m_cTimer;


    @PostConstruct
    public void init() {

        // 首次加载配置
        m_cResponseMap = new HashMap<>();
        loadResponseConfig();

        // 启动定时器，对配置定时更新
        m_cTimer = new Timer(true);
        m_cTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                loadResponseConfig();
            }
        }, 0, 5*60*1000);

        super.init();
    }

    @PreDestroy
    public void destroy() {
        // 取消定时器
        m_cTimer.cancel();

        super.destroy();
    }

    @Override
    public void process(ValidateDeviceTokenRequestMsg msg, TransportServiceCallback<ValidateDeviceCredentialsResponseMsg> callback) {
        DonAsynchron.withCallback(
                transportApiService.handle(TransportApiRequestMsg.newBuilder().setValidateTokenRequestMsg(msg).build()),
                transportApiResponseMsg -> {
                    if (callback != null) {
                        callback.onSuccess(transportApiResponseMsg.getValidateTokenResponseMsg());
                    }
                },
                getThrowableConsumer(callback), transportCallbackExecutor);
    }

    @Override
    public void process(ValidateDeviceX509CertRequestMsg msg, TransportServiceCallback<ValidateDeviceCredentialsResponseMsg> callback) {
        DonAsynchron.withCallback(
                transportApiService.handle(TransportApiRequestMsg.newBuilder().setValidateX509CertRequestMsg(msg).build()),
                transportApiResponseMsg -> {
                    if (callback != null) {
                        callback.onSuccess(transportApiResponseMsg.getValidateTokenResponseMsg());
                    }
                },
                getThrowableConsumer(callback), transportCallbackExecutor);
    }

    @Override
    public void process(GetOrCreateDeviceFromGatewayRequestMsg msg, TransportServiceCallback<GetOrCreateDeviceFromGatewayResponseMsg> callback) {
        DonAsynchron.withCallback(
                transportApiService.handle(TransportApiRequestMsg.newBuilder().setGetOrCreateDeviceRequestMsg(msg).build()),
                transportApiResponseMsg -> {
                    if (callback != null) {
                        callback.onSuccess(transportApiResponseMsg.getGetOrCreateDeviceResponseMsg());
                    }
                },
                getThrowableConsumer(callback), transportCallbackExecutor);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, SessionEventMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setSessionEvent(msg).build(), callback);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, PostTelemetryMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setPostTelemetry(msg).build(), callback);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, PostAttributeMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setPostAttributes(msg).build(), callback);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, GetAttributeRequestMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setGetAttributes(msg).build(), callback);
    }

    @Override
    public void process(SessionInfoProto sessionInfo, TransportProtos.SubscriptionInfoProto msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setSubscriptionInfo(msg).build(), callback);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, SubscribeToAttributeUpdatesMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setSubscribeToAttributes(msg).build(), callback);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, SubscribeToRPCMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setSubscribeToRPC(msg).build(), callback);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, ToDeviceRpcResponseMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setToDeviceRPCCallResponse(msg).build(), callback);
    }

    @Override
    protected void doProcess(SessionInfoProto sessionInfo, ToServerRpcRequestMsg msg, TransportServiceCallback<Void> callback) {
        forwardToDeviceActor(TransportToDeviceActorMsg.newBuilder().setSessionInfo(sessionInfo).setToServerRPCCallRequest(msg).build(), callback);
    }

    @Override
    public void process(String nodeId, DeviceActorToTransportMsg msg) {
        process(nodeId, msg, null, null);
    }

    @Override
    public void process(String nodeId, DeviceActorToTransportMsg msg, Runnable onSuccess, Consumer<Throwable> onFailure) {
        processToTransportMsg(msg);
        if (onSuccess != null) {
            onSuccess.run();
        }
    }

    private void forwardToDeviceActor(TransportToDeviceActorMsg toDeviceActorMsg, TransportServiceCallback<Void> callback) {
        TransportToDeviceActorMsgWrapper wrapper = new TransportToDeviceActorMsgWrapper(toDeviceActorMsg);
        Optional<ServerAddress> address = routingService.resolveById(wrapper.getDeviceId());
        if (address.isPresent()) {
            rpcService.tell(encodingService.convertToProtoDataMessage(address.get(), wrapper));
        } else {
            actorContext.getAppActor().tell(wrapper, ActorRef.noSender());
        }
        if (callback != null) {
            // 上读锁
            rwlock.readLock().lock();
            try {
                //TODO 返回信息修改
                String strResponse = m_cResponseMap.get(wrapper.getDeviceId().toString());
                ((DeferredResult<ResponseEntity>)callback.getRespinseWriter()).setResult(new ResponseEntity(strResponse, HttpStatus.OK));
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                rwlock.readLock().unlock();
            }
//            callback.onSuccess(null);
        }
    }

    private <T> Consumer<Throwable> getThrowableConsumer(TransportServiceCallback<T> callback) {
        return e -> {
            if (callback != null) {
                callback.onError(e);
            }
        };
    }

    // 加载自定义响应配置
    private void loadResponseConfig() {
        // 上写锁
        rwlock.writeLock().lock();
        try {
            // 清理MAP
            m_cResponseMap.clear();
            // 读取配置文件
            JsonReader reader = null;
            if (java.lang.management.ManagementFactory.getRuntimeMXBean().
                    getInputArguments().toString().indexOf("-agentlib:jdwp") > 0) {
                reader = new JsonReader(new FileReader(this.getClass().getResource("/response.json").getPath()));
            } else {
                reader = new JsonReader(new FileReader("response.json"));
            }
            JsonArray jsonArray = new JsonParser().parse(reader).getAsJsonArray();
            // 读取每项加载项
            for (JsonElement jsonElement: jsonArray) {
                m_cResponseMap.put(jsonElement.getAsJsonObject().get("id").getAsString(),
                        jsonElement.getAsJsonObject().get("respones").getAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rwlock.writeLock().unlock();
        }
    }

}
