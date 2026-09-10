package io.nexusstore.server;

import io.nexusstore.raft.Command;
import io.nexusstore.raft.CommandResult;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.concurrent.CompletableFuture;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class FileControllerTest {
    private MockMvc mvc;
    private NodeRuntime runtime;
    @BeforeEach void setup() {
        runtime = mock(NodeRuntime.class);
        mvc = MockMvcBuilders.standaloneSetup(new FileController(runtime, mock(PeerDirectory.class))).setControllerAdvice(new ApiErrors()).build();
    }
    @Test void refusesOversizedBodyBeforeConsensus() throws Exception {
        mvc.perform(put("/api/files").param("key", "large").header("X-Request-Id", "large-request")
                        .content(new byte[FileRules.MAX_FILE_BYTES + 1]))
                .andExpect(status().isPayloadTooLarge()).andExpect(header().string("X-Request-Id", "large-request"));
        verifyNoInteractions(runtime);
    }
    @Test void quorumFailureBecomes503() throws Exception {
        when(runtime.execute(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("No quorum")));
        var pending = mvc.perform(get("/api/files").param("key", "a")).andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(pending)).andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.writeOutcome").exists());
    }
    @Test void missingCommittedFileBecomes404() throws Exception {
        when(runtime.execute(any())).thenReturn(CompletableFuture.completedFuture(new CommandResult(false, null, 1)));
        var pending = mvc.perform(get("/api/files").param("key", "a")).andReturn();
        mvc.perform(asyncDispatch(pending)).andExpect(status().isNotFound());
    }
    @Test void failedPutPreservesSuppliedRequestIdForSafeRetry() throws Exception {
        when(runtime.execute(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("No quorum")));
        var pending = mvc.perform(put("/api/files").param("key", "a").header("X-Request-Id", "retry-put-1")
                        .content(new byte[] {1, 2, 3}))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(pending)).andExpect(status().isServiceUnavailable())
                .andExpect(header().string("X-Request-Id", "retry-put-1"))
                .andExpect(jsonPath("$.writeOutcome").exists());
        ArgumentCaptor<Command> command = ArgumentCaptor.forClass(Command.class);
        verify(runtime).execute(command.capture());
        assertEquals("retry-put-1", command.getValue().requestId());
    }
    @Test void failedPutReturnsGeneratedRequestIdMatchingSubmittedCommand() throws Exception {
        when(runtime.execute(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("No quorum")));
        var pending = mvc.perform(put("/api/files").param("key", "a").content(new byte[] {1}))
                .andExpect(request().asyncStarted()).andReturn();
        var response = mvc.perform(asyncDispatch(pending)).andExpect(status().isServiceUnavailable())
                .andExpect(header().exists("X-Request-Id")).andReturn().getResponse();
        ArgumentCaptor<Command> command = ArgumentCaptor.forClass(Command.class);
        verify(runtime).execute(command.capture());
        String generatedId = response.getHeader("X-Request-Id");
        assertNotNull(generatedId);
        assertDoesNotThrow(() -> java.util.UUID.fromString(generatedId));
        assertEquals(command.getValue().requestId(), generatedId);
    }
    @Test void failedDeleteAlsoPreservesRequestId() throws Exception {
        when(runtime.execute(any())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("No quorum")));
        var pending = mvc.perform(delete("/api/files").param("key", "a").header("X-Request-Id", "retry-delete-1"))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(pending)).andExpect(status().isServiceUnavailable())
                .andExpect(header().string("X-Request-Id", "retry-delete-1"));
    }
}
