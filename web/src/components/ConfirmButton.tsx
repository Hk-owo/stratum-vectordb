import { useEffect, useState } from 'react'

/**
 * 需要二次确认的按钮：第一次点击进入装弹态，5 秒内再点击确认才触发操作。
 */
export function ConfirmButton({
  label,
  confirmLabel,
  onConfirm,
  disabled,
  className,
  title,
}: {
  label: string
  confirmLabel: string
  onConfirm: () => void
  disabled?: boolean
  className?: string
  title?: string
}) {
  const [armed, setArmed] = useState(false)

  useEffect(() => {
    if (!armed) return
    const t = setTimeout(() => setArmed(false), 5000)
    return () => clearTimeout(t)
  }, [armed])

  if (!armed) {
    return (
      <button
        type="button"
        className={className}
        disabled={disabled}
        title={title}
        onClick={() => setArmed(true)}
      >
        {label}
      </button>
    )
  }

  return (
    <span className="confirm-inline">
      <button
        type="button"
        className={className === undefined ? 'danger' : `${className} danger`}
        disabled={disabled}
        onClick={() => {
          setArmed(false)
          onConfirm()
        }}
      >
        {confirmLabel}
      </button>
      <button type="button" onClick={() => setArmed(false)}>
        取消
      </button>
    </span>
  )
}
