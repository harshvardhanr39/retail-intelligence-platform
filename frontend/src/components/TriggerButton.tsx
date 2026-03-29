'use client'
import { useState } from 'react'
import { triggerPipeline } from '@/lib/api'
import { Play, Loader, CheckCircle, XCircle } from 'lucide-react'

export default function TriggerButton() {
  const [state, setState] = useState<'idle' | 'loading' | 'success' | 'error'>('idle')
  const [message, setMessage] = useState('')

  const handleTrigger = async () => {
    setState('loading')
    try {
      const result = await triggerPipeline()
      setState('success')
      setMessage(result.message ?? 'Pipeline triggered')
      setTimeout(() => setState('idle'), 4000)
    } catch (err) {
      setState('error')
      setMessage('Failed to trigger pipeline')
      setTimeout(() => setState('idle'), 4000)
    }
  }

  return (
    <div className="flex items-center gap-3">
      <button
        onClick={handleTrigger}
        disabled={state === 'loading'}
        className="flex items-center gap-2 bg-emerald-600 hover:bg-emerald-500 
                   disabled:opacity-50 disabled:cursor-not-allowed
                   text-white text-sm font-medium px-4 py-2 rounded-lg 
                   transition-colors duration-150"
      >
        {state === 'loading'
          ? <Loader className="w-4 h-4 animate-spin" />
          : <Play className="w-4 h-4" />
        }
        {state === 'loading' ? 'Triggering...' : 'Trigger Pipeline'}
      </button>

      {state === 'success' && (
        <div className="flex items-center gap-1 text-emerald-400 text-sm">
          <CheckCircle className="w-4 h-4" />
          <span>{message}</span>
        </div>
      )}
      {state === 'error' && (
        <div className="flex items-center gap-1 text-red-400 text-sm">
          <XCircle className="w-4 h-4" />
          <span>{message}</span>
        </div>
      )}
    </div>
  )
}
