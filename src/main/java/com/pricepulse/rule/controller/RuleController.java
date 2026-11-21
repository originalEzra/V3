package com.pricepulse.rule.controller;

import com.pricepulse.rule.model.AlertRule;
import com.pricepulse.rule.model.Direction;
import com.pricepulse.rule.service.RuleService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/rules")
public class RuleController {

    private final RuleService ruleService;

    public RuleController(RuleService ruleService) {
        this.ruleService = ruleService;
    }

    @GetMapping
    public List<AlertRule> listRules(@RequestParam(required = false) Long userId,
                                     @RequestParam(required = false) Long stockId,
                                     @RequestParam(required = false) String symbol) {
        return ruleService.listRules(userId, stockId, symbol);
    }

    @GetMapping("/{id}")
    public AlertRule getRule(@PathVariable Long id) {
        return ruleService.getRule(id);
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public AlertRule createRule(@Valid @RequestBody CreateRuleRequest req) {
        return ruleService.createRule(req.userId(), req.symbol(), req.direction(), req.thresholdPrice());
    }

    @PutMapping("/{id}")
    public AlertRule updateRule(@PathVariable Long id, @Valid @RequestBody UpdateRuleRequest req) {
        return ruleService.updateRule(id, req.direction(), req.thresholdPrice(), req.version());
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteRule(@PathVariable Long id, @RequestParam(required = false) Long version) {
        ruleService.deleteRule(id, version);
    }

    public record CreateRuleRequest(
            @NotNull Long userId,
            @NotBlank String symbol,
            @NotNull Direction direction,
            @NotNull @DecimalMin("0.01") BigDecimal thresholdPrice
    ) {
    }

    public record UpdateRuleRequest(
            @NotNull Direction direction,
            @NotNull @DecimalMin("0.01") BigDecimal thresholdPrice,
            Long version
    ) {
    }
}
