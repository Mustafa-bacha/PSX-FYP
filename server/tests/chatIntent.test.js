import test from 'node:test';
import assert from 'node:assert/strict';
import { __testables } from '../src/services/chatService.js';

test('detectIntent classifies greetings', () => {
  assert.equal(__testables.detectIntent('hello there'), 'greeting');
});

test('detectIntent classifies finance prompts', () => {
  assert.equal(__testables.detectIntent('Give me PSX market outlook today with risks'), 'finance');
  assert.equal(__testables.detectIntent('Analyze ANSM momentum and sentiment'), 'finance');
  assert.equal(__testables.detectIntent('what is you recommendations?'), 'finance');
  assert.equal(__testables.detectIntent('what you recommendations?'), 'finance');
  assert.equal(__testables.detectIntent('what is the current trend?'), 'finance');
});

test('detectIntent classifies off-topic prompts', () => {
  assert.equal(__testables.detectIntent('what is physics?'), 'off_topic');
  assert.equal(__testables.detectIntent('Explain chemistry in simple words'), 'off_topic');
  assert.equal(__testables.detectIntent('who is the president of pakistan?'), 'off_topic');
});

test('generic definition phrasing is blocked as off-topic', () => {
  assert.equal(__testables.detectIntent('what is quantum field theory?'), 'off_topic');
});

test('off-topic reply steers back to domain', () => {
  const reply = __testables.buildOffTopicReply({ scope: 'MARKET', question: 'what is physics?' });
  assert.match(reply, /focused on PSX market analysis/i);
  assert.match(reply, /Try asking about/i);
});
